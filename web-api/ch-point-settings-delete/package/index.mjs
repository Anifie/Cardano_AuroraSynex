import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import axios from 'axios';
import jwt from "jsonwebtoken";

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 10 * 60 * 1000 // 10 minutes in milliseconds
    }
});


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
    // console.log("nft dequeue mint event", event);
    
    

    let tableName;

    try {

        const headers = event.headers;
        let body = event.body ? JSON.parse(event.body) : {};

        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;

        if ((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda"))
            || headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com")) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);

        let token = headers['authorization'];
        console.log("token", token);

        let memberId = null;
        let member;

        if (token) {
            try {
                const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key=='JWT_SECRET').value);
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

            if (member.role !== 'ADMIN') {
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

        let DEFAULT_START_DATE = '2024-02-05T15:00:00.000Z';
        let DEFAULT_END_DATE = '2030-02-06T15:00:00.000Z';

        if(tableName == process.env.TABLE_NAME) {
            DEFAULT_START_DATE = '2025-02-06T15:00:00.000Z';
        }
        
        if(body.pointSettingsId == undefined) {
            return {
                Success: false,
                Message: "pointSettingsId is required"
            }
        }

        let sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE type = 'POINT_SETTINGS'`;
        let allPointSettingsResult = await fetchAllRecords(sql);
        let allPointSettings = allPointSettingsResult.map(unmarshall);
        
        sql = `SELECT * FROM "${tableName}" WHERE PK = 'POINT_SETTINGS' AND SK = '${body.pointSettingsId}'`;
        let pointSettingsResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(pointSettingsResult.Items.length == 0) {
            return {
                Success: false,
                Message: "pointSettingsId not found"
            }
        }

        let pointSettings = pointSettingsResult.Items.map(unmarshall)[0];

        let _typeLength = allPointSettings.filter(x => x.point_setting_type == pointSettings.point_setting_type).length;
        if(_typeLength == 1) {
            return {
                Success: false,
                Message: "You can't delete all point settings for this type"
            }
        }
        
        let txStatements = [];

        sql = `delete from "${tableName}" where PK = '${pointSettings.PK}' and SK = '${pointSettings.SK}'`;
        txStatements.push({ "Statement": sql});

        
        let prevPointSettings = allPointSettings.find(x => x.end_date == pointSettings.start_date);
        let nextPointSettings = allPointSettings.find(x => x.start_date == pointSettings.end_date);

        if(nextPointSettings) {
            let prevDate;
            if(prevPointSettings) {
                prevDate = prevPointSettings.end_date;
            }
            else {
                prevDate = DEFAULT_START_DATE;
            }
            
            let _sql = `update "${tableName}" SET modified_date = '${new Date().toISOString()}' , start_date = '${prevDate}' where PK = '${nextPointSettings.PK}' and SK = '${nextPointSettings.SK}'`;
            txStatements.push({ "Statement": _sql});
        }
        else {
            let nextDate = DEFAULT_END_DATE;

            let _sql = `update "${tableName}" SET modified_date = '${new Date().toISOString()}' , end_date = '${nextDate}' where PK = '${prevPointSettings.PK}' and SK = '${prevPointSettings.SK}'`;
            txStatements.push({ "Statement": _sql});
        }

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("Transaction result", dbTxResult);
        
        return {
            Success: true
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-point-settings-delete ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-point-settings-delete - ' + random10DigitNumber,
            Message: `Error in ch-point-settings-delete : ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };
        
        // if(tableName == process.env.TABLE_NAME)
        //     await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: e.message
            //Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};