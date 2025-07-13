import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
// import OpenAI from 'openai';
// import axios from 'axios';
import jwt from 'jsonwebtoken';
import { ulid } from 'ulid';
import pkg from 'pg';
import { stringify } from "csv-stringify/sync";
const { Pool } = pkg;

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const s3Client = new S3Client({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION,
  maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
  requestHandler: {
      requestTimeout: 1 * 60 * 1000 // 1 minutes in milliseconds
  }
});
// const openai = new OpenAI({
//      apiKey: process.env.OPENAI_APIKEY // This is also the default, can be omitted
// });

let tableName;
let configs;

const pool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DATABASE,
    password: process.env.POSTGRES_PASSSWORD,
    port: 5432 // Default PostgreSQL port
});

function sortByTimestampAscending(arr) {
  return arr.sort((a, b) => a.timestamp - b.timestamp);
}

async function fetchAllRecords(sql) {
    let results = [];
    let nextToken;

    do {
        const command = new ExecuteStatementCommand({
            Statement: sql,
            NextToken: nextToken, // Add NextToken if available
        });

        const response = await dbClient.send(command);

        // Accumulate items from this page
        if (response.Items) {
            results = results.concat(response.Items);
        }

        // Update nextToken for the next iteration
        nextToken = response.NextToken;
    } while (nextToken); // Continue until there's no nextToken

    return results;
}

async function fetchChatMessages(isTest, startDate, endDate) {
  // const query = `
  //   select du.id as user_id, du.username, dm.channel_id, dc.name as channel_name, dm."timestamp", dm.id as message_id, dm."content" as message 
  //   from honda.discord_message dm 
  //       inner join honda.discord_user du on dm.author_id = du.id 
  //       inner join honda.discord_channel dc on dc.id = dm.channel_id 
  //   where du.username not in ('synergy_lab_official','MEE6', 'HondaNFTBot', 'katousan.', 'anifie_tang', 'Ticket Tool')
  //       and dc.name not in ('test-room', 'team-chat', 'test-all', 'moderator-only', '🔔｜ミント完了通知')
  //       and dm."content" <> ''
  //   order by dm."timestamp" ;
  // `;

  // const result = await pool.query(query);

  let _startDate = startDate ? startDate : (isTest ? process.env.MESSAGE_START_DATE_TEST : process.env.MESSAGE_START_DATE);
  let _endDate = endDate ? endDate : new Date().toISOString();

  let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_MESSAGE' and created_date between '${_startDate}' and '${_endDate}' order by created_date`;
  console.log("sql", sql);
  let result = await fetchAllRecords(sql);
  if (result.length === 0) {
    return [[], _startDate, _endDate]
  }

  result = result.map(unmarshall);
  
  return [result, _startDate, _endDate];
}

const analyzeMessage = async (instruction, messages, isTest) => {
  const lambdaParams = {
      FunctionName: `ch-analytic-chat-analyze`,
      InvocationType: 'RequestResponse',
      LogType: 'Tail',
      Payload: JSON.stringify({ // Convert Payload to JSON string
        Instruction: instruction,
        Messages: messages,
        isTest: isTest
      })
  };

  console.log("lambdaParams", lambdaParams);

  try {
      const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
      console.log("lambdaResult", lambdaResult);

      console.log('lambdaResult.payload', Buffer.from(lambdaResult.Payload).toString());
      
      const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
      console.log("payload", payload);

      if (payload.errorMessage) {
          console.error("Analyze message error message:", JSON.stringify(payload.errorMessage));
          throw new Error('Analyze message Lambda error: ' + JSON.stringify(payload.errorMessage));
      }

      console.log("analyzeResult", payload);
      return payload;
  } catch (error) {
      console.error("Analyze message Lambda invocation error:", error);
      throw new Error('Lambda invocation failed: ' + error.message);
  }
};

async function uploadJsonToS3(uniqueFileName, jsonObj, bucketName, folderName) {
  // Convert JSON array to a string
  const jsonString = JSON.stringify(jsonObj);

  // Generate a unique file name
  const fileName = `${folderName}/${uniqueFileName}.json`;

  try {
      // Upload JSON file to S3
      const command = new PutObjectCommand({
          Bucket: bucketName,
          Key: fileName,
          Body: jsonString,
          ContentType: "application/json", // Set content type for JSON
      });

      await s3Client.send(command);

      // Construct the HTTP URL
      const httpUrl = `https://${bucketName}.s3.${process.env.AWS_REGION}.amazonaws.com/${fileName}`;
      console.log("File uploaded to:", httpUrl);

      return httpUrl;
  } catch (error) {
      console.error("Error uploading JSON to S3:", error);
      throw new Error("Failed to upload JSON to S3.");
  }
}

async function uploadCSVToS3(uniqueFileName, csvObj, bucketName, folderName) {

  // Generate a unique file name
  const fileName = `${folderName}/${uniqueFileName}.csv`;

  try {
      // Upload JSON file to S3
      const command = new PutObjectCommand({
          Bucket: bucketName,
          Key: fileName,
          Body: csvObj,
          ContentType: "text/csv",
      });

      await s3Client.send(command);

      // Construct the HTTP URL
      const httpUrl = `https://${bucketName}.s3.${process.env.AWS_REGION}.amazonaws.com/${fileName}`;
      console.log("File uploaded to:", httpUrl);

      return httpUrl;
  } catch (error) {
      console.error("Error uploading CSV to S3:", error);
      throw new Error("Failed to upload CSV to S3.");
  }
}

export const handler = async (event) => {
  console.log("event", event);

  try {
    var headers = event.headers;
    var body = {};

    if(event.body)
        body = JSON.parse(event.body);    

    console.log("origin", headers['origin']);
    tableName = process.env.TABLE_NAME_TEST;
    const domainProdArray = process.env.DOMAIN_PROD.split(',');
    if (domainProdArray.some(domain => headers['origin'] === domain)) {
        tableName = process.env.TABLE_NAME;
    }
    console.log("tableName", tableName);

    let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
    configs = configResult.Items.map(item => unmarshall(item));
    console.log("configs", configs);

    let token = headers['authorization'];
    console.log("token", token);

    let memberId = null;
    let member;

    if (!body.appPubKey && token) {
        try {
            const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key == 'JWT_SECRET').value);
            console.log("decoded", decoded);

            memberId = decoded.MemberId;

            if (Date.now() >= decoded.exp * 1000) {
                return {
                    Success: false,
                    Message: "Token expired"
                };
            }
        } catch (e) {
            console.log("error verify token", e);
            return {
                Success: false,
                Message: "Invalid token."
            };
        }

        let sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBER_ID#${memberId}' AND type = 'MEMBER' AND begins_with("PK", 'MEMBER#')`;
        let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if (memberResult.Items.length === 0) {
            console.log("member not found: " + memberId);
            return {
                Success: false,
                Message: "member not found: " + memberId
            };
        }
        member = memberResult.Items.map(item => unmarshall(item))[0];

        if (!member.role?.includes('ADMIN')) {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }
    } else {
        return {
            Success: false,
            Message: "Missing login info"
        };
    }

    if(!body.instruction) {
      return {
        Success: false,
        Message: 'instruction is required'
      }
    }
    
    if(!body.analysisType) {
      return {
        Success: false,
        Message: 'analysisType is required'
      }
    }

    const msgResult = await fetchChatMessages(tableName == process.env.TABLE_NAME_TEST, body.startDate, body.endDate);
    console.log("msgResult", msgResult);

    let messages = msgResult[0];

    if (messages && messages.length === 0) {
      console.log("No messages to analyze.");
      return {
        Success: false,
        Message: 'No messages to analyze'
      }
    }

    console.log(`Fetched 1 ${messages.length} messages.`);

    let omittedChannelIds = process.env.OMITTED_CHANNEL_IDS;
    if(omittedChannelIds) {
      let omittedChannelIdsArray = omittedChannelIds.split(',');
      messages = messages.filter(x => !omittedChannelIdsArray.includes(x.discord_channel_id))
    }

    console.log(`Fetched 2 ${messages.length} messages.`);

    if(body.analysisType == 'MESSAGE') {

      if(messages.length > 1000) {
        return {
          Success: false,
          Message: `Chat data is more than 1000 messages (${messages.length}). Please split into smaller batch of messages.`
        }
      }
      // if(messages.length > 1000 && !body.approvalPassword) {
      //   return {
      //     Success: false,
      //     Message: 'Chat data is too large. Please provide approval password.'
      //   }
      // }
      // else if(messages.length > 1000 && body.approvalPassword) {
      //   if(body.approvalPassword == process.env.APPROVAL_PASSWORD) {
      //     console.log("approvalPassword correct");
      //   }
      // }

      let _id = ulid();

      //'user_messages': '${JSON.stringify(userMessages)}',
      let sql = `INSERT INTO "${tableName}" 
                VALUE {
                'PK': 'ANALYTIC_BATCH#${_id}',
                'SK': 'ANALYTIC_BATCH',
                'type': 'ANALYTIC_BATCH',
                'status': 'PROCESSING',
                'start_date': '${msgResult[1]}',
                'end_date': '${msgResult[2]}',
                'batch_id': '${_id}',
                'instruction': '${body.instruction}',
                'analysis_type': '${body.analysisType}',
                'created_date': '${new Date().toISOString()}'
                }`;
      let insertBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
      console.log("insertBatchResult", insertBatchResult);

      let messageAnalyzeResult = [];

      try {
        for (let i = 0; i < messages.length; i++) {
          let msg = messages[i];

          const analysis = await analyzeMessage(body.instruction, msg.content, tableName == process.env.TABLE_NAME_TEST);
          console.log('analysis', analysis);

          messageAnalyzeResult.push({
            user_id: msg.discord_user_id,
            message_id: msg.discord_message_id,
            message_content: msg.content,
            channel_id: msg.discord_channel_id,
            timestamp: msg.timestamp,
            result: analysis
          })

        }  
      } catch (_err) {
        console.log(_err);      
      }

      if(messageAnalyzeResult.length > 0) {
        messageAnalyzeResult.sort((a,b) => {
          const enthusiasmA = a.result?.enthusiasm ?? -Infinity;
          const enthusiasmB = b.result?.enthusiasm ?? -Infinity;
          const knowledgeA = a.result?.knowledge_depth ?? -Infinity;
          const knowledgeB = b.result?.knowledge_depth ?? -Infinity;

          if (enthusiasmA !== enthusiasmB) {
            return enthusiasmB - enthusiasmA; // Sort enthusiasm in descending order
          }

          return knowledgeB - knowledgeA; // Sort knowledge_depth in descending order
        })

        let inputCSV = stringify(messages, { header: true });
        let flattenOutput = messageAnalyzeResult.map(({ user_id, message_id, channel_id, message_content, timestamp, result }) => ({
                                                                          user_id,
                                                                          channel_id,
                                                                          message_id,
                                                                          message_content,
                                                                          timestamp,
                                                                          enthusiasm: result.enthusiasm,
                                                                          knowledge_depth: result.knowledge_depth,
                                                                          related_user_ids: result.related_user_ids.join("\n")
                                                                        }));
        let outputCSV = stringify(flattenOutput, { header: true });

        let inputJsonFilePath = await uploadJsonToS3("input_" + _id, messages, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
        let outputJsonFilePath = await uploadJsonToS3("output_" + _id, messageAnalyzeResult, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
        let inputCSVFilePath = await uploadCSVToS3("input_" + _id, inputCSV, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
        let outputCSVFilePath = await uploadCSVToS3("output_" + _id, outputCSV, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');

        sql = `UPDATE "${tableName}" 
              SET status = 'DONE', 
              modified_date = '${new Date().toISOString()}' , 
              input_path = '${inputJsonFilePath}' ,
              output_path = '${outputJsonFilePath}',
              input_csv_path = '${inputCSVFilePath}' ,
              output_csv_path = '${outputCSVFilePath}'  
              where PK = 'ANALYTIC_BATCH#${_id}' and SK = 'ANALYTIC_BATCH'`;
        console.log("sql", sql);
        let updateBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchResult", updateBatchResult);
      }
      else {
        sql = `UPDATE "${tableName}" 
              SET status = 'FAILED', 
              modified_date = '${new Date().toISOString()}'
              where PK = 'ANALYTIC_BATCH#${_id}' and SK = 'ANALYTIC_BATCH'`;
        let updateBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchResult", updateBatchResult);
      }

    }
    else if (body.analysisType == 'USER') {

      const uniqueUsers = Array.from(
              new Map(
                messages.map(({ discord_user_id }) => [discord_user_id])
              ).entries()
          ).map(([discord_user_id]) => ({ discord_user_id }));

      console.log("uniqueUsers", uniqueUsers);

      let userMessages = [];

      for (let i = 0; i < uniqueUsers.length; i++) {
          const user = uniqueUsers[i];
          let filteredMessages = messages.filter(x=>x.discord_user_id == user.discord_user_id && x.content).map(m => ({
            channel_id: m.discord_channel_id,
            // channel_name: m.channel_name,
            message_id: m.discord_message_id,
            message: m.content,
            timestamp: m.timestamp
        }))
        filteredMessages = sortByTimestampAscending(filteredMessages);
        let userMessage = {
          index: i,
          user_id: user.discord_user_id,
          messages: filteredMessages.map(x => x.timestamp + ' ' + x.message).join('\n')
        };

        if(userMessage.messages)
          userMessages.push(userMessage);
      }

      if(userMessages.length == 0) {
        return {
          Success: false,
          Message: 'Chat data not found'
        }
      }

      let _id = ulid();

      //'user_messages': '${JSON.stringify(userMessages)}',
      let sql = `INSERT INTO "${tableName}" 
                VALUE {
                'PK': 'ANALYTIC_BATCH#${_id}',
                'SK': 'ANALYTIC_BATCH',
                'type': 'ANALYTIC_BATCH',
                'status': 'PROCESSING',
                'start_date': '${msgResult[1]}',
                'end_date': '${msgResult[2]}',
                'batch_id': '${_id}',
                'instruction': '${body.instruction}',
                'analysis_type': '${body.analysisType}',
                'created_date': '${new Date().toISOString()}'
                }`;
      let insertBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
      console.log("insertBatchResult", insertBatchResult);

      let userMessageAnalyzeResult = [];

      try {
        for (let i = 0; i < userMessages.length; i++) {

          const analysis = await analyzeMessage(body.instruction, userMessages[i].messages);
          console.log('analysis', analysis);

          userMessageAnalyzeResult.push({
            user_id: userMessages[i].user_id,
            messages: userMessages[i].messages,
            result: analysis
          })

        }  
      } catch (_err) {
        console.log(_err);      
      }

      if(userMessageAnalyzeResult.length > 0) {

      userMessageAnalyzeResult.sort((a,b) => {
        const enthusiasmA = a.result?.enthusiasm ?? -Infinity;
        const enthusiasmB = b.result?.enthusiasm ?? -Infinity;
        const knowledgeA = a.result?.knowledge_depth ?? -Infinity;
        const knowledgeB = b.result?.knowledge_depth ?? -Infinity;

        if (enthusiasmA !== enthusiasmB) {
          return enthusiasmB - enthusiasmA; // Sort enthusiasm in descending order
        }

        return knowledgeB - knowledgeA; // Sort knowledge_depth in descending order
      })

      let inputCSV = stringify(userMessages, { header: true });
      let flattenOutput = userMessageAnalyzeResult.map(({ user_id, messages, result }) => ({
                                                                        user_id,
                                                                        messages,
                                                                        enthusiasm: result.enthusiasm,
                                                                        knowledge_depth: result.knowledge_depth,
                                                                        related_user_ids: result.related_user_ids.join("\n")
                                                                      }));
      let outputCSV = stringify(flattenOutput, { header: true });

      let inputJsonFilePath = await uploadJsonToS3("input_" + _id, userMessages, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
      let outputJsonFilePath = await uploadJsonToS3("output_" + _id, userMessageAnalyzeResult, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
      let inputCSVFilePath = await uploadCSVToS3("input_" + _id, inputCSV, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');
      let outputCSVFilePath = await uploadCSVToS3("output_" + _id, outputCSV, configs.find(x => x.key == 'S3_BUCKET').value, 'chat_analysis');

      sql = `UPDATE "${tableName}" 
            SET status = 'DONE', 
            modified_date = '${new Date().toISOString()}' , 
            input_path = '${inputJsonFilePath}' ,
            output_path = '${outputJsonFilePath}',
            input_csv_path = '${inputCSVFilePath}' ,
            output_csv_path = '${outputCSVFilePath}'  
            where PK = 'ANALYTIC_BATCH#${_id}' and SK = 'ANALYTIC_BATCH'`;
        console.log("sql", sql);
        let updateBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchResult", updateBatchResult);
      }
      else {
        sql = `UPDATE "${tableName}" 
              SET status = 'FAILED', 
              modified_date = '${new Date().toISOString()}'
              where PK = 'ANALYTIC_BATCH#${_id}' and SK = 'ANALYTIC_BATCH'`;
        let updateBatchResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchResult", updateBatchResult);
      }

    }
    
  } catch (e) {
    const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
    console.error('error in ch-analytic-chat ' + random10DigitNumber, e);

    const message = {
        Subject: 'Error - ch-analytic-chat- ' + random10DigitNumber,
        Message: `Error in ch-analytic-chat ${e.message}\n\nStack trace:\n${e.stack}`,
        TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
    };
    
    if(tableName == process.env.TABLE_NAME)
        await snsClient.send(new PublishCommand(message));

    return {
        Success: false,
        Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
    };
  }
};