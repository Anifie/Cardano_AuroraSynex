const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const { DynamoDBClient, ExecuteStatementCommand } = require("@aws-sdk/client-dynamodb");
const client = new DynamoDBClient({ region: process.env.AWS_REGION });
const axios = require("axios");
var jwt = require('jsonwebtoken');
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();

async function ToCommentViewModel(obj){
    
    let result = {
                    Sender: {
                        MemberId: obj.sender_id,
                        DisplayName: obj.sender_display_name,
                        AvatarURL: obj.sender_avatar_url
                    },
                    CommentId: obj.comment_id,
                    Message: obj.message,
                    ReplyCommentId: obj.reply_to_comment_id,
                    ContractAddress: obj.contract_address,
                    TokenId: obj.token_id,
                    ArtworkId: obj.artwork_id,
                    Status: obj.status,
                    LikedCount: obj.liked_count,
                    CreatedDate: obj.created_date
                };
                
    return result;
}

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

export const handler = async (event) => {
    
    console.log("comment listing get event", event);
    
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
            // sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
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
        
        // if(!body.blogId){
        //     return {
        //         Success: false,
        //         Code: 2,
        //         Message: 'Missing blogId',
        //     };
        // }
        

        // if(body.messageId) {
        //     if (!body.chatId) {
        //         const response = {
        //             Success: false,
        //             Code: 2,
        //             Message: "chatId is required to filter by messageId"
        //         };
        //         return response;
        //     }
        // }

        // if(body.chatId) {
        //     if (!body.messageId) {
        //         const response = {
        //             Success: false,
        //             Code: 2,
        //             Message: "messageId is required to filter by chatId"
        //         };
        //         return response;
        //     }
        // }

        sql = `select * from "${tableName}"."${process.env.TABLE_NAME_GSI_BY_TYPE_DATE}" where type = 'COMMENT' `;
        
        // if(body.messageId) {
        //     let msgSql = `select * from "${process.env.TABLE_NAME_XBAND}" where type = 'MESSAGE' and PK = 'CHAT#${body.chatId}' and BEGINS_WITH("SK", 'MESSAGE#${body.messageId}')`;
        //     console.log("msgSql", msgSql);
        //     let msgResult = await db.executeStatement({Statement: msgSql}).promise();
        //     if(msgResult.Items.length == 0) {
        //         console.log("message not found: " + body.messageId);
        //         const response = {
        //             Success: false,
        //             Code: 2,
        //             Message: "message not found: " + body.messageId
        //         };
        //         return response;
        //     }
        //     let msg = msgResult.Items.map(unmarshall)[0];
        //     sql = `select * from "${process.env.TABLE_NAME_XBAND}"."InvertedIndex" where type = 'COMMENT' and SK = '${msg.SK}'`;
        // }
        
        if(member.role === 'ADMIN') {  // current user is admin
            if(body.status && body.status != '') {
                sql += ` AND status = '${body.status}'`;
            }    
        }
        else {
            sql += ` AND status = 'ACTIVE'`;
        }

        if(body.commentId && body.commentId != '')
            sql += ` AND comment_id = '${body.commentId}'`;

        if(body.contractAddress && body.tokenId)
            sql += ` AND PK = 'ASSET#${body.contractAddress}#${body.tokenId}'`;

        if(body.artworkId)
            sql += ` AND PK = 'ARTWORK#${body.artworkId}'`;

        if(body.senderId && body.senderId != '')
            sql += ` AND sender_id = '${body.senderId}'`;

        if(body.lastKey && body.lastKey != '')
            sql += ` AND SK < '${body.lastKey}'`;

        sql += ` ORDER BY created_date DESC`;
                
        console.log("sql", sql);
        
        if(!body.pageSize)
            body.pageSize = 10;
        
        var nextToken = null;
        var allComments = [];
        var maxAttemps = 40;    // max page size
        var attempt = 0;
        var commentsResult = null;
        while(attempt < maxAttemps) {
            commentsResult = await client.send(
                                                    new ExecuteStatementCommand({
                                                        Statement: sql,
                                                        NextToken: nextToken,
                                                        Limit: +body.pageSize
                                                    })
                                                );

            console.log("commentsResult", JSON.stringify(commentsResult));
            console.log("commentsResult.NextToken", commentsResult.NextToken);
            console.log("commentsResult.LastEvaluatedKey", commentsResult.LastEvaluatedKey);
            
            nextToken = commentsResult.NextToken;
        
            var messages = commentsResult.Items.map(unmarshall);
            allComments.push(...messages);
            console.log("allComments", JSON.stringify(allComments));
            console.log("allComments length", allComments.length);
            console.log("attempt", attempt);

            attempt++;
            
            if(commentsResult.NextToken == null 
                || commentsResult.NextToken == undefined
                || allComments.length >= body.pageSize)
                break;
        }

        if(allComments.length > 0) {            
            
            // get sender info
            let senderPKs = allComments.map(x => "'MEMBER#" + x.sender_id + "'").filter(onlyUnique);

            sql = `SELECT * FROM "${tableName}"
                    WHERE type = 'MEMBER'`;

            if(senderPKs.length > 0)
                sql += ` AND PK IN (${senderPKs.join(", ")})`;

            console.log("sql", sql);
            
            let senderResult = await db.executeStatement({
                                                            Statement: sql,
                                                        }).promise();

            if(senderResult.Items.length > 0) {
                let senders = senderResult.Items.map(unmarshall);
                console.log("senders", senders);
                // replace display_name in allMessages
                allComments = allComments.map(x => {
                                if(senders.filter(y => y.user_id == x.sender_id).length > 0) {
                                    return {
                                            ...x, 
                                            sender_display_name: senders.filter(y => y.user_id == x.sender_id)[0]?.display_name,
                                            sender_avatar_url: senders.filter(y => y.user_id == x.sender_id)[0].avatar_uri ? senders.filter(y => y.user_id == x.sender_id)[0].avatar_uri : `https://i.pravatar.cc/150?u=${x.sender_id}`
                                    };
                                }
                                else {
                                    return {...x, sender_display_name: "", sender_avatar_url: ""};
                                }
                            });
            }
            else {
                console.log("no sender found. senderSKs : ", senderPKs);
            }
        }

        let _allComments = await Promise.all(allComments.map(async(a) => await ToCommentViewModel(a)));
        
        const response = {
            Success: true,
            Code: 0,
            Data: { 
                    comments: _allComments, 
                    lastKey: commentsResult.LastEvaluatedKey 
                }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-comment-listing-get ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-comment-listing-get - ' + random10DigitNumber,
            Message: `Error in ch-comment-listing-get: ${e.message}\n\nStack trace:\n${e.stack}`,
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