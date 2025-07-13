import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
// import { ulid } from 'ulid';
import axios from 'axios';
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import md5 from 'md5';
import QRCode from 'qrcode';
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

        if (body.appPubKey === undefined && body.uuid === undefined && token) {
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

            if (body.memberId === undefined) {
                return {
                    Success: false,
                    Message: "memberId is required"
                };
            }

            sql = `SELECT * FROM "${process.env.TABLE_NAME}" WHERE PK = 'MEMBER#${body.memberId}' AND type = 'MEMBER'`;
            memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if (memberResult.Items.length === 0) {
                console.log("member not found: " + body.memberId);
                return {
                    Success: false,
                    Message: "member not found: " + body.memberId
                };
            }
            member = memberResult.Items.map(item => unmarshall(item))[0];
        } else if (body.appPubKey) {
            try {
                const idToken = token.split(' ')[1] || "";
                const jwks = jose.createRemoteJWKSet(new URL("https://api.openlogin.com/jwks"));
                const jwtDecoded = await jose.jwtVerify(idToken, jwks, { algorithms: ["ES256"] });
                console.log("jwtDecoded", JSON.stringify(jwtDecoded));

                if ((jwtDecoded.payload).wallets[0].public_key === body.appPubKey) {
                    console.log("Validation Success");
                } else {
                    console.log("Validation Failed");
                    return {
                        Success: false,
                        Code: 1,
                        Message: "Validation failed"
                    };
                }

                let userId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier);
                console.log("userId", userId);

                let memberResult = await dbClient.send(new ExecuteStatementCommand({
                    Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`,
                    Parameters: [{ S: 'MEMBER#' + userId }]
                }));
                if (memberResult.Items.length === 0) {
                    return {
                        Success: false,
                        Message: 'member not found',
                    };
                }
                member = memberResult.Items.map(item => unmarshall(item))[0];
            } catch (e) {
                console.log("error verify token", e);
                return {
                    Success: false,
                    Code: 1,
                    Message: "Invalid token."
                };
            }
        } else if (body.uuid && body.token) {
            try {
                const _response = await axios.post(process.env.PARTICLE_API_URL,
                    {
                        jsonrpc: "2.0",
                        id: 0,
                        method: "getUserInfo",
                        params: [body.uuid, body.token],
                    },
                    {
                        auth: {
                            username: process.env.PARTICLE_PROJECT_ID,
                            password: process.env.PARTICLE_SERVER_KEY,
                        },
                    }
                );

                if (_response.data.error) {
                    console.log("Error verify user info", _response.data.error);
                    throw new Error(JSON.stringify(_response.data.error));
                }

                let memberResult = await dbClient.send(new ExecuteStatementCommand({
                    Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`,
                    Parameters: [{ S: 'MEMBER#' + body.uuid }]
                }));
                if (memberResult.Items.length === 0) {
                    return {
                        Success: false,
                        Message: 'member not found',
                    };
                }
                member = memberResult.Items.map(item => unmarshall(item))[0];
            } catch (e) {
                console.error(e);
                return {
                    Success: false,
                    Message: e.message
                };
            }
        } else {
            return {
                Success: false,
                Message: "Missing login info"
            };
        }

        let asset;
        if (body.contractAddress && body.tokenId) {
            let assetResult = await dbClient.send(new ExecuteStatementCommand({
                Statement: `SELECT * FROM "${tableName}" WHERE PK = ? AND type = 'ASSET'`,
                Parameters: [{ S: 'ASSET#' + body.contractAddress + "#" + body.tokenId }]
            }));
            if (assetResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: "asset not found"
                };
            }
            asset = assetResult.Items.map(item => unmarshall(item))[0];
        } else {
            return {
                Success: false,
                Code: 2,
                Message: 'Missing NFT info',
            };
        }

        if (asset.owner_address !== member.wallet_address) {
            return {
                Success: false,
                Message: 'NFT is not owned by this member'
            };
        }

        let qrURL;
        if (asset.coffee_qr_image_url) {
            qrURL = asset.coffee_qr_image_url;
        } else {
            const data = JSON.stringify({
                contractAddress: asset.contract_address,
                tokenId: asset.token_id,
                walletAddress: member.wallet_address,
                timestamp: new Date().getTime()
            });
            let signature = crypto.createHmac('sha256', process.env.QR_SECRET_KEY).update(data).digest('hex');
            const qrData = `${data}.${signature}`;
            qrURL = await QRCode.toDataURL(qrData);

            let sql = `UPDATE "${tableName}" SET coffee_qr_image_url = '${qrURL}' WHERE PK = '${asset.PK}' AND SK = '${asset.SK}'`;
            await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        }

        return {
            Success: true,
            Data: { coffeeQRURL: qrURL }
        };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-coffee-qr-get ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-coffee-qr-get - ' + random10DigitNumber,
            Message: `Error in ch-coffee-qr-get: ${e.message}\n\nStack trace:\n${e.stack}`,
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