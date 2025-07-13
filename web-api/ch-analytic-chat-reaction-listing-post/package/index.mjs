import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
// import OpenAI from 'openai';
// import axios from 'axios';
import jwt from 'jsonwebtoken';
// import ULID from 'ulid';
import pkg from 'pg';
const { Pool } = pkg;

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
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
const pool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DATABASE,
    password: process.env.POSTGRES_PASSSWORD,
    port: 5432 // Default PostgreSQL port
});

function ToDiscordReaction(obj) {
    console.log("ToDiscordReaction", obj);
    
    let msg = {
        DiscordUserId: obj.discord_user_id,
        ChannelId: obj.discord_channel_id,
        // Timestamp: obj.timestamp,
        MessageId: obj.discord_message_id,
        EmojiName: obj.emoji_name,
        Reactions: obj.raw_reactions ? JSON.parse(obj.raw_reactions) : undefined,
        CreatedDate: obj.created_date
    };

    return msg;
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


    if(!body.pageSize) {
        body.pageSize = 10;
    }
    
    let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_REACTION' order by created_date DESC`;

    let allMessages = [];
    let nextToken = body.nextToken;

    const maxAttempts = 40;
    let attempt = 0;

    while (attempt < maxAttempts) {
        const messagesResult = await dbClient.send(
            new ExecuteStatementCommand({
                Statement: sql,
                NextToken: nextToken,
                Limit: +body.pageSize
            })
        );

        nextToken = messagesResult.NextToken;
        const messages = messagesResult.Items.map(unmarshall);
        allMessages.push(...messages);

        attempt++;

        if (!nextToken || allMessages.length >= body.pageSize) break;
    }
    
    //let _messages = await Promise.all(allMessages.map(async(a) => await ToMessage(a, tableName)));
    
    return {
      Success: true,
      Data: {
        messages: allMessages.map(x => ToDiscordReaction(x)),
        nextToken: nextToken
      }
    }    

  } catch (e) {
    const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
    console.error('error in ch-analytic-chat-reaction-listing-post ' + random10DigitNumber, e);

    const message = {
        Subject: 'Error - ch-analytic-chat-reaction-listing-post- ' + random10DigitNumber,
        Message: `Error in ch-analytic-chat-reaction-listing-post ${e.message}\n\nStack trace:\n${e.stack}`,
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