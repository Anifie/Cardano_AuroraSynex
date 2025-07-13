import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import crypto from 'crypto';

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

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

            // if (body.memberId === undefined) {
            //     return {
            //         Success: false,
            //         Message: "memberId is required"
            //     };
            // }

            // sql = `SELECT * FROM "${process.env.TABLE_NAME}" WHERE PK = 'MEMBER#${body.memberId}' AND type = 'MEMBER'`;
            // memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // if (memberResult.Items.length === 0) {
            //     console.log("member not found: " + body.memberId);
            //     return {
            //         Success: false,
            //         Message: "member not found: " + body.memberId
            //     };
            // }
            // member = memberResult.Items.map(item => unmarshall(item))[0];
        } else {
            return {
                Success: false,
                Message: "Missing login info"
            };
        }

        if(!body.qrCode) {
            return {
                Success: false,
                Message: "qrCode is required"
            };
        }

        let arr = body.qrCode.split('.');
        let data = arr[0];
        let signature = arr[1];

        //verify signature
        const expectedSignature = crypto.createHmac('sha256', process.env.QR_SECRET_KEY).update(data).digest('hex');
        if(expectedSignature !== signature) {
            console.log("Invalid signature");
            return {
                Success: false,
                Message: "Invalid signature or is being tampered"
            };
        }

        let jsonData = JSON.parse(data);

        let assetResult = await dbClient.send(new ExecuteStatementCommand({
            Statement: `SELECT * FROM "${tableName}" WHERE PK = ? AND type = 'ASSET'`,
            Parameters: [{ S: 'ASSET#' + jsonData.contractAddress + "#" + jsonData.tokenId }]
        }));
        if (assetResult.Items.length === 0) {
            return {
                Success: false,
                Message: "asset not found"
            };
        }
        let asset = assetResult.Items.map(item => unmarshall(item))[0];

        if(asset.coffee_claim_status === 'CLAIMED') {
            const claimedDate = new Date(asset.coffee_claim_date);
            const diffInMilliseconds = new Date() - claimedDate;
            const diffInSeconds = diffInMilliseconds / 1000;
            if(diffInSeconds > 10) {
                console.log("Coffee is already claimed");
                return {
                    Success: false,
                    Message: "Coffee is already claimed"
                };
            }
        }

        let sql = `UPDATE "${tableName}" SET coffee_claim_status = 'CLAIMED' , coffee_claim_date = '${new Date().toISOString()}' WHERE PK = '${asset.PK}' AND SK = '${asset.SK}'`;
        let updateResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateResult", updateResult);
        
        return {
            Success: true
        };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-coffee-claim-qr-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-coffee-claim-qr-post - ' + random10DigitNumber,
            Message: `Error in ch-coffee-claim-qr-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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