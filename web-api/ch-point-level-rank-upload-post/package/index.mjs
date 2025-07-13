import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
  
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

        if(!body.levelRankData) {
            return {
                Success: false,
                Message: "levelRankData is required"
            }
        }

        let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'LEVEL_RANK'`;

        let existingLevelRankResult = await fetchAllRecords(sql);
        console.log("existingLevelRankResult", existingLevelRankResult);

        if(existingLevelRankResult.length > 0) {
            existingLevelRankResult = existingLevelRankResult.map(unmarshall);
            for (let i = 0; i < existingLevelRankResult.length; i++) {
                const item = existingLevelRankResult[i];
                sql = `delete from "${tableName}" where PK = '${item.PK}' and SK = '${item.SK}'`;
                let deleteResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("deleteResult", deleteResult);
            }
        }

        for (let i = 0; i < body.levelRankData.length; i++) {
            const row = body.levelRankData[i];
            console.log("i : ", i);
            console.log(row);

            sql = `INSERT INTO "${tableName}" 
                    VALUE { 
                    'PK': 'LEVEL_RANK', 
                    'SK': '${i + 1}', 
                    'type': 'LEVEL_RANK', 
                    'level': '${row.Level}', 
                    'point': '${row.Point}', 
                    'rank': '${row.Rank}', 
                    'created_date': '${new Date().toISOString()}'
                    }`;

            let insertResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("insertResult", insertResult);
        }

        return {
                    Success: true
                };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-point-level-rank-upload-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-point-level-rank-upload-post - ' + random10DigitNumber,
            Message: `Error in ch-point-level-rank-upload-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };
    
        if (tableName === process.env.TABLE_NAME) {
            await snsClient.send(new PublishCommand(message));
        }
    
        return {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
    }
};    