import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import axios from 'axios';
import jwt from "jsonwebtoken";
import ULID from 'ulid';

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

        if(body.pointSettingType == undefined) {
            return {
                Success: false,
                Message: "pointSettingType is required"
            }
        }

        if(body.pointSettingType == 'DATE' && body.cutOffDate == undefined) {
            return {
                Success: false,
                Message: "cutOffDate is required"
            }
        }

        let sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE type = 'POINT_SETTINGS' and SK <> 'POINT_SETTINGS'`;
        let pointSettingsResult = await fetchAllRecords(sql);
        let pointSettings = pointSettingsResult.map(unmarshall);

        let txStatements = [];

        if(body.pointSettingType == 'DATE') {
            for (let i = 0; i < pointSettings.length; i++) {
                const pointSetting = pointSettings[i];
                
                if(body.cutOffDate > pointSetting.start_date && body.cutOffDate < pointSetting.end_date) {
                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , end_date = '${body.cutOffDate}' where PK = '${pointSetting.PK}' and SK = '${pointSetting.SK}'`;
                    txStatements.push({ "Statement": sql});
    
                    let _id = ULID.ulid();
                    sql = `INSERT INTO "${tableName}" 
                        VALUE { 
                            'PK': 'POINT_SETTINGS' , 
                            'SK': '${_id}', 
                            'type': 'POINT_SETTINGS', 
                            'point_setting_type': '${body.pointSettingType}', 
                            'start_date': '${body.cutOffDate}', 
                            'end_date': '${pointSetting.end_date}', 
                            'weight_message': ${pointSetting.weight_message}, 
                            'weight_reaction': ${pointSetting.weight_reaction}, 
                            'weight_vote': ${pointSetting.weight_vote} , 
                            'omitted_channel_ids': '${pointSetting.omitted_channel_ids}' ,
                            'message_minimum_length': ${pointSetting.message_minimum_length},
                            'weight_attachments': ${pointSetting.weight_attachments},
                            'weight_replies': ${pointSetting.weight_replies},
                            'created_date': '${new Date().toISOString()}'
                        }`;
                    txStatements.push({ "Statement": sql});
    
                    const statements = { "TransactStatements": txStatements };  
                    console.log("statements", JSON.stringify(statements));
                    const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
                    console.log("Transaction result", dbTxResult);
    
                    return {
                        Success: true
                    }
                }
            }
    
            return {
                Success: false,
                Message: 'Invalid cutoff date'
            }
        }
        else {
            let otherPointSetting = pointSettings.find(x => x.point_setting_type == body.pointSettingType);
            if (!otherPointSetting) {

                let _id = ULID.ulid();
                sql = `INSERT INTO "${tableName}" 
                    VALUE { 
                        'PK': 'POINT_SETTINGS' , 
                        'SK': '${_id}', 
                        'type': 'POINT_SETTINGS', 
                        'point_setting_type': '${body.pointSettingType}', 
                        'weight_role': '${pointSetting.weight_role}',
                        'weight_nft': ${pointSetting.weight_nft},
                        'created_date': '${new Date().toISOString()}'
                    }`;
                txStatements.push({ "Statement": sql});

                const statements = { "TransactStatements": txStatements };  
                console.log("statements", JSON.stringify(statements));
                const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
                console.log("Transaction result", dbTxResult);

                return {
                    Success: true
                }
            }

            return {
                Success: false,
                Message: 'pointSettingType already exist'
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-point-settings-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-point-settings-post - ' + random10DigitNumber,
            Message: `Error in ch-point-settings-post : ${e.message}\n\nStack trace:\n${e.stack}`,
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