const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const ULID = require('ulid');
const axios = require("axios");
var jwt = require('jsonwebtoken');
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();
const QRCode = require('qrcode');
const crypto = require('crypto');

export const handler = async (event) => {
    console.log("event", event);
    
    let tableName;

    try {
        

        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);

        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);

        var token = headers['authorization'];
        console.log("token", token);
        
        let memberId = null;
        let member;

        if(body.appPubKey == undefined && body.uuid == undefined && token) {
            //verify token
            try{
                const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key=='JWT_SECRET').value);
                console.log("decoded", decoded);
                
                memberId = decoded.MemberId;
                
                if (Date.now() >= decoded.exp * 1000) {
                    const response = {
                        Success: false,
                        Message: "Token expired"
                    };
                    return response;
                }
            }catch(e){
                console.log("error verify token", e);
                const response = {
                    Success: false,
                    Message: "Invalid token."
                };
                return response;
            }

            let sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBER_ID#${memberId}' and type = 'MEMBER' and begins_with("PK", 'MEMBER#')`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + memberId);
                const response = {
                    Success: false,
                    Message: "member not found: " + memberId
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];

            if(member.role !== 'ADMIN') {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }


            // replace member with member who we want to sent the NFT to
            if(body.memberId == undefined) {
                return {
                    Success: false,
                    Message: "memberId is required"
                };
            }
            sql = `select * from "${process.env.TABLE_NAME}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + body.memberId);
                const response = {
                    Success: false,
                    Message: "member not found: " + body.memberId
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];
        }
        else if(body.appPubKey) {

            var token = headers['authorization'];
            console.log("token", token);

            if(!token)  {
                console.log('missing authorization token in headers');
                const response = {
                        Success: false,
                        Code: 1,
                        Message: "Unauthorize user"
                    };
                return response;
            }

            let userId;
            // let aggregateVerifier;
        
            //verify token
            try{
                const idToken = token.split(' ')[1] || "";
                const jwks = jose.createRemoteJWKSet(new URL("https://api.openlogin.com/jwks"));
                const jwtDecoded = await jose.jwtVerify(idToken, jwks, {
                                                                            algorithms: ["ES256"],
                                                                        });
                console.log("jwtDecoded", JSON.stringify(jwtDecoded));
        
                if ((jwtDecoded.payload).wallets[0].public_key == body.appPubKey) {
                    // Verified
                    console.log("Validation Success");
                } else {
                    // Verification failed
                    console.log("Validation Failed");
                    return {
                        Success: false,
                        Code: 1,
                        Message: "Validation failed"
                    };
                }
                
                userId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier)
                console.log("userId", userId);
                
                // aggregateVerifier = jwtDecoded.payload.aggregateVerifier;
                
            }catch(e){
                console.log("error verify token", e);
                const response = {
                    Success: false,
                    Code: 1,
                    Message: "Invalid token."
                };
                return response;
            }

            let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}).promise();
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        else if (body.uuid && body.token) {
            
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

            console.log(_response.data);

            if(_response.data.error) {
                console.log("Error verify user info", _response.data.error);
                throw new Error(JSON.stringify(_response.data.error))
            }

            let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + body.uuid }],}).promise();
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        else {
            console.log('Missing login info');
            const response = {
                    Success: false,
                    Message: "Missing login info"
                };
            return response;
        }

        let asset;

        if(body.contractAddress && body.tokenId){
            let assetResult = await db.executeStatement({
                                                            Statement: `SELECT * FROM "${tableName}" WHERE PK = ? AND type = 'ASSET'`,
                                                            Parameters: [
                                                                            {S: 'ASSET#' + body.contractAddress + "#" + body.tokenId }
                                                                        ]}).promise();
            if(assetResult.Items.length == 0) {
                console.log("asset not found: " + body.contractAddress + ' ' + body.tokenId);
                return {
                    Success: false,
                    Message: "asset not found: " + body.contractAddress + ' ' + body.tokenId
                };
            }
            asset = assetResult.Items.map(unmarshall)[0];
            console.log("asset", asset);   
        }
        else {
            return {
                Success: false,
                Code: 2,
                Message: 'Missing nft info',
            };
        }
        
        if(asset.owner_address != member.wallet_address) {
            return {
                Success: false,
                Message: 'NFT does not owned by this member'
            }
        }

        let qrURL;

        if(asset.coffee_qr_image_url) {
            qrURL = asset.coffee_qr_image_url;
        }
        else {

            const data = JSON.stringify({
                                            contractAddress: asset.contract_address,
                                            tokenId: asset.token_id,
                                            walletAddress: member.wallet_address,
                                            timestamp: new Date().getTime()
                                        });
            console.log("data", data);
            let signature = crypto.createHmac('sha256', process.env.QR_SECRET_KEY).update(data).digest('hex');
            const qrData = `${data}.${signature}`;
            console.log("qrData", qrData);
            qrURL = await QRCode.toDataURL(qrData);
            console.log("qrURL", qrURL);

            sql = `update "${tableName}" set coffee_qr_image_url = '${qrURL}' where PK = '${asset.PK}' and SK = '${asset.SK}'`;
            let updateQRResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("updateQRResult", updateQRResult);
        }

        return {
            Success: true,
            Data: {
                coffeeQRURL: qrURL
            }
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-coffee-qr-get ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-coffee-qr-get - ' + random10DigitNumber,
            Message: `Error in ch-coffee-qr-get: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};