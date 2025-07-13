import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });


async function ToQueueViewModel(obj){
    return {
        MemberId: obj.user_id,
        MemberId: obj.user_id,
        WalletAddress: obj.wallet_address,
        QueueType: obj.queue_type,
        QueueId: obj.queue_id,
        DiscordUserId: obj.discord_user_id,
        DiscordUserIdReal: obj.discord_user_id_real,
        AppPubKey: obj.app_pub_key,
        Token: obj.token,
        NFTType: obj.nft_type,
        Status: obj.status,
        CreatedDate: obj.created_date,
        ModifiedDate: obj.modified_date,
        ArtworkId: obj.artworkId,
        ArtworkId2: obj.artworkId2,
        Result: obj.data,
        NFTResult: obj.result,
        TokenId: obj.token_id,
        ChatMemberId: obj.chat_member_id
    };
}

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

export const handler = async (event) => {
    console.log("nft enqueue mint event", event);
    
    let tableName;
    let configs;

    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);    

        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME_TEST;
        const domainProdArray = process.env.DOMAIN_PROD.split(',');
        if (domainProdArray.some(domain => headers['origin'] === domain)) {
            tableName = process.env.TABLE_NAME;
        }
        console.log("tableName", tableName);
        let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
        configs = configResult.Items.map(unmarshall);
        console.log("configs", configs);

        var token = headers['authorization'];
        console.log("token", token);
        
        let memberId = null;
        let member;

        if(body.appPubKey == undefined && token) {
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

            if(!member.role?.includes('ADMIN')) {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }
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

            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}))
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
            return {
                Success: false,
                Code: 1,
                Message: "Missing login info."
            };
        }
        
        let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'QUEUE'`;

        if(body.queueId) {
            sql += ` and queue_id = '${body.queueId}'`;
        }

        if(body.queueType) {
            sql += ` and queue_type = '${body.queueType}'`;
        }

        if(body.nftType) {
            sql += ` and nft_type = '${body.nftType}'`;
        }

        if(body.memberId) {
            sql += ` and user_id = '${body.memberId}'`;
        }
        else if(body.appPubKey) {
            sql += ` and user_id = '${member.user_id}'`;
        }

        if(body.status) {
            sql += ` and status = '${body.status}'`;
        }

        if(body.lastKey && body.lastKey != '')
            sql += ` AND created_date < '${body.lastKey}'`;

        sql += ` order by created_date desc`;

        console.log("sql", sql);

        if(!body.pageSize)
            body.pageSize = 10;
        
        var nextToken = null;
        var allQueues = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var queuesResult = null;
        while (attempt < maxAttempts) {
            queuesResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = queuesResult.NextToken;
            const queues = queuesResult.Items.map(unmarshall);
            allQueues.push(...queues);

            attempt++;

            if (!nextToken || allQueues.length >= body.pageSize) break;
        }
        
        let transformedQueue = await Promise.all(allQueues.map(async(a) => await ToQueueViewModel(a)));    

        if(transformedQueue.length > 0) {

            // find member info
            var memberIds = transformedQueue.map(x => "'" + x.MemberId + "'").filter(onlyUnique);
            console.log("memberIds", JSON.stringify(memberIds));
            var statementSellOrder = `select * from "${tableName}"."ByTypeCreatedDate" 
                                    where type = 'MEMBER' AND user_id in (${memberIds.join(", ")})`;
            var membersResult = await dbClient.send(new ExecuteStatementCommand({ Statement: statementSellOrder }));
            console.log("membersResult", membersResult);
            var members = membersResult.Items.map(unmarshall);
            console.log("members", members);
            

            transformedQueue.forEach(async(s) => {
                if(members.filter(x=>x.user_id == s.MemberId).length == 0) {
                    var _sql = `select * from "${tableName}" where type = 'MEMBER' AND PK = 'MEMBER#${s.MemberId}'`;
                    console.log(_sql);
                    var singleMemberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: _sql }));
                    console.log("singleMemberResult", singleMemberResult);
                    var singleMember = membersResult.Items.map(unmarshall);
                    console.log("singleMember", singleMember);

                    // s.CampaignCode = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.campaign_code;
                    s.MemberTokenId = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.nft_member_token_id;
                    // s.MemberBTokenId = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.nft_member_b_token_id;
                    // s.MemberCreatedDate = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.created_date;
                    s.WalletAddress = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.wallet_address;
                    s.AvatarURI = singleMember.filter(x=>x.user_id == s.MemberId)[0]?.avatar_uri;
                }
                else {
                    // s.CampaignCode = members.filter(x=>x.user_id == s.MemberId)[0]?.campaign_code;
                    s.MemberTokenId = members.filter(x=>x.user_id == s.MemberId)[0]?.nft_member_token_id;
                    // s.MemberBTokenId = members.filter(x=>x.user_id == s.MemberId)[0]?.nft_member_b_token_id;
                    // s.MemberCreatedDate = members.filter(x=>x.user_id == s.MemberId)[0]?.created_date;
                    s.WalletAddress = members.filter(x=>x.user_id == s.MemberId)[0]?.wallet_address;
                    s.AvatarURI = members.filter(x=>x.user_id == s.MemberId)[0]?.avatar_uri;
                }
            });
            console.log("transformedQueue after", transformedQueue);

        }

        const response = {
            Success: true,
            Code: 0,
            Data: { 
                    queues: transformedQueue, 
                    nextToken: nextToken
                }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-queue-listing-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Ada Error - ch-nft-queue-listing-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-queue-listing-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
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