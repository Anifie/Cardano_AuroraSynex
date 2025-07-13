import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import OpenAI from 'openai';
import axios from 'axios';
import jwt from 'jsonwebtoken';
import https from 'https';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

let tableName;
let configs;

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
    

    let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_MESSAGE'`;
    let messagesResult = await fetchAllRecords(sql);
    let messages = messagesResult.map(unmarshall);



    // Process timestamps into Day vs. Hour bins
    let timeBasedHeatmapData = Array.from({ length: 7 }, () => Array(24).fill(0));    
    messages.forEach(row => {
      let date = new Date(row.timestamp);
      let day = date.getUTCDay();   // Ensure using UTC (0 = Sunday, 6 = Saturday)
      let hour = date.getUTCHours(); // Ensure using UTC (0 - 23)
      timeBasedHeatmapData[day][hour]++;
    });


    
    // Aggregate messages (Count per User per Hour)
    let userActivity = {};

    messages.forEach(row => {
      let user = row.discord_user_id;
      let timestamp = row.timestamp;
      let hour = timestamp.substring(11, 13); // Extract "HH" from "YYYY-MM-DDTHH:MM:SS.sssZ"

      if (!userActivity[user]) {
          userActivity[user] = Array(24).fill(0);
      }
      userActivity[user][parseInt(hour, 10)]++;
    });

    // Get top 50 most active users
    let sortedUsers = Object.keys(userActivity)
    .sort((a, b) => userActivity[b].reduce((sum, n) => sum + n, 0) -
                    userActivity[a].reduce((sum, n) => sum + n, 0))
    .slice(0, 50);

    let top50ActiveUserHourHeatMap = sortedUsers.map(user => userActivity[user]);




    // Aggregate messages (Count per Channel per Hour)
    let channelActivity = {};

    messages.forEach(row => {
        let channel = row.discord_channel_id;
        let timestamp = row.timestamp;
        let hour = timestamp.substring(11, 13); // Extract "HH" from timestamp

        if (!channelActivity[channel]) {
            channelActivity[channel] = Array(24).fill(0);
        }
        channelActivity[channel][parseInt(hour, 10)]++;
    });

    // Get top 50 most active channels
    let sortedChannels = Object.keys(channelActivity)
        .sort((a, b) => channelActivity[b].reduce((sum, n) => sum + n, 0) -
                        channelActivity[a].reduce((sum, n) => sum + n, 0))
        .slice(0, 50);

    let top50ActiveChannelHeatMapData = sortedChannels.map(channel => channelActivity[channel]);




    return {
      Success: true,
      Data: {
        timeBasedAxisZ: timeBasedHeatmapData,
        timeBasedAxisY: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
        timeBasedAxisX: Array.from({ length: 24 }, (_, i) => `${i}:00`), // Hours

        top50ActiveUsersHourAxisZ: top50ActiveUserHourHeatMap,
        top50ActiveUsersHourAxisY: sortedUsers,
        top50ActiveUsersHourAxisX: Array.from({ length: 24 }, (_, i) => `${i}:00`),

        top50ActiveChannelsAxisZ: top50ActiveChannelHeatMapData,
        top50ActiveChannelsAxisY: sortedChannels,
        top50ActiveChannelsAxisX: Array.from({ length: 24 }, (_, i) => `${i}:00`)
      }
    };


  } catch (e) {
    const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
    console.error('error in ch-analytic-chat-data-visualize-heatmap ' + random10DigitNumber, e);

    const message = {
        Subject: 'Error - ch-analytic-chat-data-visualize-heatmap - ' + random10DigitNumber,
        Message: `Error in ch-analytic-chat-data-visualize-heatmap ${e.message}\n\nStack trace:\n${e.stack}`,
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