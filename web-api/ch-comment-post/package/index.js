const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const ULID = require('ulid');
const axios = require("axios");
var jwt = require('jsonwebtoken');
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();

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
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
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


            // // replace member with member who we want to sent the NFT to
            // if(body.memberId == undefined) {
            //     return {
            //         Success: false,
            //         Message: "memberId is required"
            //     };
            // }
            // sql = `select * from "${process.env.TABLE_NAME}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            // memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // if(memberResult.Items.length == 0) {
            //     console.log("member not found: " + body.memberId);
            //     const response = {
            //         Success: false,
            //         Message: "member not found: " + body.memberId
            //     };
            //     return response;
            // }
            // member = memberResult.Items.map(unmarshall)[0];
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


        let artwork;
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
        else if (body.artworkId) {
            let sql = `select * from "${tableName}" where PK = '${'ARTWORK#' + body.artworkId}' and type = 'ARTWORK'`;
            let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(artworkResult.Items.length == 0) {
                console.log("artwork not found: " + body.artworkId);
                return {
                    Success: false,
                    Message: "artwork not found: " + body.artworkId
                };
            }
            artwork = artworkResult.Items.map(unmarshall)[0];
            console.log("artwork", artwork);
        }
        else {
            return {
                Success: false,
                Code: 2,
                Message: 'Missing artwork or nft info',
            };
        }
        
        if(!body.message){
            return {
                Success: false,
                Code: 2,
                Message: 'Missing message',
            };
        }
        
        let now = new Date().toISOString();
        if(now < process.env.COMMENT_START_DATE) {
            return {
                Success: false,
                essage: "この機能は現在有効になっていません"   //this feature is not currently enabled
            };
        }        

        let sender = null;
        
        if(body.senderId) {
            if(member.role !== 'ADMIN') {
                return {
                    Success: false,
                    Code: 2,
                    Message: 'Only admin can set senderId',
                };            
            }
            else {
                let sql = `select * from "${tableName}" where PK = 'MEMBER#${body.senderId}' and type = 'MEMBER'`;
                let senderResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                if(senderResult.Items.length == 0) {
                    console.log("sender not found: " + body.senderId);
                    const response = {
                        Success: false,
                        Code: 2,
                        Message: "sender not found: " + body.senderId
                    };
                    return response;
                }
                sender = senderResult.Items.map(unmarshall)[0];
            }
        }

        let txStatements = [];

        let commentId = ULID.ulid().toUpperCase();
        console.log('inserting new comment');
        console.log("senderId", body.senderId);
        console.log("member.user_id", member.user_id);

        let newCommentSql = `INSERT INTO "${tableName}" 
                            VALUE {
                                    'PK': '${asset ? asset.PK : artwork.PK}',
                                    'SK': '${'COMMENT#' + commentId}',
                                    'type': 'COMMENT',
                                    'comment_id': '${commentId}',`;
                                    
        if(asset) {
            newCommentSql += ` 'contract_address': '${asset.contract_address}' , 'token_id': '${asset.token_id}', 'asset_id': '${asset.asset_id}', `;
        }
        else if(artwork) {
            newCommentSql += ` 'artwork_id': '${artwork.artwork_id}', `;
        }

        newCommentSql += `'sender_id': '${body.senderId ? body.senderId : member.user_id}',
                            'reply_to_comment_id': '${body.replyToCommentId ? body.replyToCommentId : ''}',
                            'message': '${body.message ? body.message : ""}',
                            'status': 'ACTIVE',
                            'liked_count': 0,
                            'created_date': '${new Date().toISOString()}',
                            'created_by': '${member.user_id}'
                        }`;
        console.log(newCommentSql);
        txStatements.push({ "Statement": newCommentSql});

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("add comment dbResult", dbTxResult);

        const response = {
            Success: true,
            Code: 0,
            Data: {
                commentId: commentId
            }
        };        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-comment-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-comment-post - ' + random10DigitNumber,
            Message: `Error in ch-comment-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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