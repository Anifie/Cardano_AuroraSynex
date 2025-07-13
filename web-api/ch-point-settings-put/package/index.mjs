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
    console.log("point setting put event", event);
    
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

        // if(body.weightMessage == undefined) {
        //     return {
        //         Success: false,
        //         Message: "weightMessage is required"
        //     };
        // }

        // if(body.weightReaction == undefined) {
        //     return {
        //         Success: false,
        //         Message: "weightReaction is required"
        //     };
        // }

        // if(body.weightVote == undefined) {
        //     return {
        //         Success: false,
        //         Message: "weightVote is required"
        //     };
        // }

        // if(body.weightRole == undefined) {
        //     return {
        //         Success: false,
        //         Message: "weightRole is required"
        //     }
        // }

        // if(body.levelVar1 == undefined) {
        //     return {
        //         Success: false,
        //         Message: "levelVar1 is required"
        //     }
        // }

        // if(body.levelVar2 == undefined) {
        //     return {
        //         Success: false,
        //         Message: "levelVar2 is required"
        //     }
        // }

        // if(body.startDate == undefined) {
        //     return {
        //         Success: false,
        //         Message: "startDate is required"
        //     }
        // }

        // if(body.endDate == undefined) {
        //     return {
        //         Success: false,
        //         Message: "endDate is required"
        //     }
        // }

        if(body.pointSettingsId == undefined) {
            return {
                Success: false,
                Message: "pointSettingsId is required"
            }
        }

        let sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE type = 'POINT_SETTINGS' and SK <> 'POINT_SETTINGS'`;
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

        let txStatements = [];

        // let pointSettings = pointSettingsResult.Items.map(unmarshall);
        // , level_var_1 = ${body.levelVar1}
        // , level_var_2 = ${body.levelVar2}
        sql = `UPDATE "${tableName}" 
                SET modified_date = '${new Date().toISOString()}' `;

        if(body.startDate) {
            sql += `, start_date = '${body.startDate}' `;

            let prevPointSettings = allPointSettings.find(x => x.end_date == pointSettings.start_date);
            if(prevPointSettings) {
                let _sql = `update "${tableName}" SET modified_date = '${new Date().toISOString()}' , end_date = '${body.startDate}' where PK = '${prevPointSettings.PK}' and SK = '${prevPointSettings.SK}'`;
                txStatements.push({ "Statement": _sql});
            }
        }

        if(body.endDate) {
            sql += `, end_date = '${body.endDate}' `;

            let nextPointSettings = allPointSettings.find(x => x.start_date == pointSettings.end_date);
            if(nextPointSettings) {
                let _sql = `update "${tableName}" SET modified_date = '${new Date().toISOString()}' , start_date = '${body.endDate}' where PK = '${nextPointSettings.PK}' and SK = '${nextPointSettings.SK}'`;
                txStatements.push({ "Statement": _sql});
            }
        }
        
        if(body.weightMessage != undefined) {
            sql += `, weight_message = ${body.weightMessage} `;
        }

        if(body.weightReaction != undefined) {
            sql += `, weight_reaction = ${body.weightReaction} `;
        }

        if(body.weightVote != undefined) {
            sql += `, weight_vote = ${body.weightVote} `;
        }

        if(body.weightRole != undefined) {
            sql += `, weight_role = '${typeof body.weightRole == 'object' ? JSON.stringify(body.weightRole) : body.weightRole}' `;
        }

        if(body.omittedChannelIds != undefined) {
            sql += `, omitted_channel_ids = '${body.omittedChannelIds ? body.omittedChannelIds : ''}' `;
        }

        if(body.messageMinimumLength != undefined) {
            sql += `, message_minimum_length = ${body.messageMinimumLength != undefined ? body.messageMinimumLength : 0} `;
        }

        if(body.weightAttachments != undefined) {
            sql += `, weight_attachments = ${body.weightAttachments != undefined ? body.weightAttachments : 0} `;
        }
        
        if(body.weightNFT != undefined) {
            sql += `, weight_nft = ${body.weightNFT} `;
        }

        if(body.weightReplies != undefined) {
            sql += `, weight_replies = ${body.weightReplies} `;
        }

        sql += ` WHERE PK = '${pointSettings.PK}' AND SK = '${pointSettings.SK}'`;

        txStatements.push({ "Statement": sql});

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("Transaction result", dbTxResult);
        
        return {
            Success: true
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-point-settings-put ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-point-settings-put - ' + random10DigitNumber,
            Message: `Error in ch-point-settings-put : ${e.message}\n\nStack trace:\n${e.stack}`,
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