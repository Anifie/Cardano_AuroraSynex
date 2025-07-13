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


            // replace member with member
            if(body.memberId == undefined) {
                return {
                    Success: false,
                    Message: "memberId is required"
                };
            }
            sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
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

        if(!body.commentId) {
            return {
                Success: false,
                Message: 'commentId is required'
            }
        }

        let sql = `select * from "${tableName}"."InvertedIndex" where SK = '${'COMMENT#' + body.commentId}' and type = 'COMMENT'`;
        console.log("sql", sql);
        let commentResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(commentResult.Items.length == 0) {
            console.log("comment not found: " + body.commentId);
            return {
                Success: false,
                Message: "comment not found: " + body.commentId
            };
        }
        let comment = commentResult.Items.map(unmarshall)[0];
        console.log("comment", comment);
        
        sql = `select * from "${tableName}" where PK = 'COMMENTLIKE#${member.user_id}' and SK = '${'COMMENT#' + comment.comment_id}'`;
        let commentLikeResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(commentLikeResult.Items.length == 0) {
            console.log("Comment like not found");
            return {
                Success: false,
                Message: "Comment like not found"
            }
        }

        let txStatements = [];

        sql = `update "${tableName}" set liked_count = '${parseInt(comment.liked_count ? comment.liked_count : 0) - 1}' , modified_date = '${new Date().toISOString()}' where PK = '${comment.PK}' and SK = '${comment.SK}'`;
        txStatements.push({ "Statement": sql});

        sql = `delete from "${tableName}" where PK = 'COMMENTLIKE#${member.user_id}' and SK = '${comment.SK}'`;
        txStatements.push({ "Statement": sql});

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("remove like comment dbResult", dbTxResult);

        return {
            Success: true
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-comment-like-delete ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-comment-like-delete - ' + random10DigitNumber,
            Message: `Error in ch-comment-like-delete: ${e.message}\n\nStack trace:\n${e.stack}`,
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