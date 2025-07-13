import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

async function ToBatch(obj) {
    
  let batch = {
    Status: obj.status,
    BatchId: obj.batch_id,
    Instruction: obj.instruction,
    //   Result: obj.result ? JSON.parse(obj.result) : undefined,
    //   UserMessages: obj.user_messages ? JSON.parse(obj.user_messages) : undefined,
    InputPath: obj.input_path,
    OutputPath: obj.output_path,
    ModifiedDate: obj.modified_date,
    CreatedDate: obj.created_date,
    InputCSVPath: obj.input_csv_path,
    OutputCSVPath: obj.output_csv_path,
    StartDate: obj.start_date,
    EndDate: obj.end_date,
    AnalysisType: obj.analysis_type ? obj.analysis_type : 'USER',
  };

  return batch;
}



export const handler = async (event) => {
    console.log("event", event);

    let tableName;
    let configs;

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

        let singleRecord = false;

        let sql = `SELECT analysis_type, input_path, output_path, input_csv_path, output_csv_path, start_date, end_date, status, batch_id, instruction, result, created_date, modified_date `;

        if(body.includeMessages) {
          sql += ` , user_messages `;
        }
          
        sql += ` FROM "${tableName}"."ByTypeCreatedDate" WHERE type = 'ANALYTIC_BATCH' order by created_date DESC`;

        if(body.batchId) {
            sql = ` SELECT analysis_type, input_path, output_path, input_csv_path, output_csv_path, start_date, end_date, status, batch_id, instruction, result, created_date, modified_date `;
            
            if(body.includeMessages) {
              sql += ` , user_messages `;
            }

            sql += ` FROM "${tableName}" WHERE PK = 'ANALYTIC_BATCH#${body.batchId}' and SK = 'ANALYTIC_BATCH'`; 
        }

        console.log("sql", sql);
        
        let allBatches = [];
        let nextToken = body.nextToken;

        if (!body.pageSize) 
            body.pageSize = 10;

        if(singleRecord) {
            let result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            allBatches = result.Items.map(item => unmarshall(item));
        }
        else {
            const maxAttempts = 40;
            let attempt = 0;

            while (attempt < maxAttempts) {
                const batchesResult = await dbClient.send(
                    new ExecuteStatementCommand({
                        Statement: sql,
                        NextToken: nextToken,
                        Limit: +body.pageSize
                    })
                );

                nextToken = batchesResult.NextToken;
                const batches = batchesResult.Items.map(unmarshall);
                allBatches.push(...batches);

                attempt++;

                if (!nextToken || allBatches.length >= body.pageSize) break;
            }
        }
        
        let _batches = await Promise.all(allBatches.map(async(a) => await ToBatch(a, tableName)));

        return {
            Success: true,
            Data: {
                batches: _batches,
                nextToken: nextToken
            }
        };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-analytic-chat-batch-listing-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-analytic-chat-batch-listing-post ' + random10DigitNumber,
            Message: `Error in ch-analytic-chat-batch-listing-post ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
    
        return {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
    }
};    