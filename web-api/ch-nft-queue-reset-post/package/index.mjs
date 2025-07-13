import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 1 * 60 * 1000 // 1 minutes in milliseconds
    }
});

let tableName;
let configs;

const checkOwnerPost = async (params, origin) => {
    console.log("checkOwnerPost", params);
    let response = await axios.post(configs.find(x=>x.key=='API_URL').value+ '/nft/owner',
                        JSON.stringify(params),
                        {
                            headers: {
                                'Content-Type': 'application/json',
                                'origin': origin
                            }
                        }
                    );
    console.log('checkOwnerPost jsonResult', response.data);
    return response.data;
}

const revoke = async (params) => {
    let lambdaParams = {
        FunctionName: 'ch-web3',
        InvocationType: 'RequestResponse', 
        LogType: 'Tail',
        Payload: {
            action: params.action,
            isTest: params.isTest
        }
    };            
    const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
    const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
    
    if (payload.errorMessage) {
        console.error("lambda error message:", JSON.stringify(payload.errorMessage));
        throw new Error('Lambda error: ' + JSON.stringify(payload.errorMessage));
    }

    console.log("result", payload);
    return payload;
    
    // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
    // console.log("lambdaParams", lambdaParams);            
    // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
    // console.log("lambdaResult", lambdaResult);            
    // if(lambdaResult.Payload.errorMessage) {
    //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
    //     throw new Error('Upload folder Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
    // }            
    // let uploadResult = JSON.parse(lambdaResult.Payload);    
    // console.log("revoke result", uploadResult);
    // return uploadResult;
}

export const handler = async (event) => {
    console.log("nft queue reset post event", event);
    
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

        //if(body.appPubKey == undefined && token) {
        if(token) {
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
        // else if(body.appPubKey) {

        //     var token = headers['authorization'];
        //     console.log("token", token);

        //     if(!token)  {
        //         console.log('missing authorization token in headers');
        //         const response = {
        //                 Success: false,
        //                 Code: 1,
        //                 Message: "Unauthorize user"
        //             };
        //         return response;
        //     }

        //     let userId;
        //     // let aggregateVerifier;
        
        //     //verify token
        //     try{
        //         const idToken = token.split(' ')[1] || "";
        //         const jwks = jose.createRemoteJWKSet(new URL("https://api.openlogin.com/jwks"));
        //         const jwtDecoded = await jose.jwtVerify(idToken, jwks, {
        //                                                                     algorithms: ["ES256"],
        //                                                                 });
        //         console.log("jwtDecoded", JSON.stringify(jwtDecoded));
        
        //         if ((jwtDecoded.payload).wallets[0].public_key == body.appPubKey) {
        //             // Verified
        //             console.log("Validation Success");
        //         } else {
        //             // Verification failed
        //             console.log("Validation Failed");
        //             return {
        //                 Success: false,
        //                 Code: 1,
        //                 Message: "Validation failed"
        //             };
        //         }
                
        //         userId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier)
        //         console.log("userId", userId);
                
        //         // aggregateVerifier = jwtDecoded.payload.aggregateVerifier;
                
        //     }catch(e){
        //         console.log("error verify token", e);
        //         const response = {
        //             Success: false,
        //             Code: 1,
        //             Message: "Invalid token."
        //         };
        //         return response;
        //     }

        //     let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}).promise();
        //     console.log("memberResult", JSON.stringify(memberResult));
        //     if(memberResult.Items.length === 0) {
        //         return {
        //             Success: false,
        //             Message: 'member not found',
        //         };
        //     }

        //     member = memberResult.Items.map(unmarshall)[0];
        // }
        // else if (body.uuid && body.token) {
            
        //     const _response = await axios.post(process.env.PARTICLE_API_URL,
        //         {
        //             jsonrpc: "2.0",
        //             id: 0,
        //             method: "getUserInfo",
        //             params: [body.uuid, body.token],
        //         },
        //         {
        //             auth: {
        //                 username: process.env.PARTICLE_PROJECT_ID,
        //                 password: process.env.PARTICLE_SERVER_KEY,
        //             },
        //         }
        //     );

        //     console.log(_response.data);

        //     if(_response.data.error) {
        //         console.log("Error verify user info", _response.data.error);
        //         throw new Error(JSON.stringify(_response.data.error))
        //     }

        //     let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + body.uuid }],}).promise();
        //     console.log("memberResult", JSON.stringify(memberResult));
        //     if(memberResult.Items.length === 0) {
        //         return {
        //             Success: false,
        //             Message: 'member not found',
        //         };
        //     }

        //     member = memberResult.Items.map(unmarshall)[0];
        // }
        else {
            return {
                Success: false,
                Code: 1,
                Message: "Missing login info."
            };
        }

        if(!body.queueId) {
            return {
                Success: false,
                Message: 'queueId is required'
            };
        }


        let sql;

        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'QUEUE' and queue_id = '${body.queueId}' order by created_date desc`;
        console.log("sql", sql);
        let queueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(queueResult.Items.length > 0) {
            let queueItem = queueResult.Items.map(unmarshall)[0];

            if(queueItem.status == 'SUCCESS') {
                return {
                    Success: false,
                    Message: 'Queue already succes, cant reset'
                }
            }

            console.log("original token", queueItem.token);
            console.log("original app_pub_key", queueItem.app_pub_key);

            //use admin token

            if(queueItem.queue_type == 'MINT_QUEUE' && queueItem.nft_type.includes('MEMBER_') && body.isForcePreregister === true) {
                console.log("isForcePreregister is true");
                sql = `update "${tableName}" set status = 'NEW', data = '', app_pub_key = '', token = '${token}', force_pregister = true , modified_date = '${new Date().toISOString()}' where PK = '${queueItem.PK}' and SK = '${queueItem.SK}'`;

                let resetQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("resetQueueResult", resetQueueResult);
            }
            else if(queueItem.queue_type == 'MINT_QUEUE' && queueItem.nft_type.includes('MEMBER_')  && body.isForceRegular === true) {
                console.log("isForceRegular is true");
                sql = `update "${tableName}" set status = 'NEW', data = '', app_pub_key = '', token = '${token}', force_regular = true , modified_date = '${new Date().toISOString()}' where PK = '${queueItem.PK}' and SK = '${queueItem.SK}'`;

                let resetQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("resetQueueResult", resetQueueResult);
            }
            else if(queueItem.queue_type == 'MINT_QUEUE' && body.isForce === true) {
                console.log("isForce is true");

                if(queueItem.result) {
                    return {
                        Success: false,
                        Message: 'queue item already have result: ' + queueItem.result
                    }
                }

                let nftType = queueItem.nft_type;
                let userId = queueItem.user_id;
                let walletAddress = queueItem.wallet_address;

                sql = `select * from "${tableName}"."InvertedIndex" where type = 'ASSET' and SK = 'MEMBERWALLET#${walletAddress}' and status = 'MINTING'`;
                let mintingAssetResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                if(mintingAssetResult.Items.length == 0) {
                    return {
                        Success: false,
                        Message: 'Minting asset not found'
                    }
                }
                let mintingAsset = mintingAssetResult.Items.map(unmarshall)[0];
                console.log("mintingAsset", mintingAsset);
                let tokenIdElement = JSON.parse(mintingAsset.metadata).find(x=>x.trait_type == 'ID');
                if(!tokenIdElement) {
                    return {
                        Success: false,
                        Message: 'tokenId not found in metadata'
                    }
                }
                let tokenId = tokenIdElement.value;

                let ownerResult = await checkOwnerPost({
                    nftType: nftType,
                    tokenId: tokenId
                }, headers['origin']);

                if(ownerResult.Success) {
                    if(ownerResult.Data.owner == walletAddress){
                        let action;
                        if(nftType == 'MEMBER') {
                            action = 'REVOKE_MEMBER';
                        }
                        else if(nftType == 'CHATDATA') {
                            action = 'REVOKE_CHATDATA';
                        }
                        else if(nftType == 'ART'){
                            action = 'REVOKE_METAVERSE';
                        }
                        else {
                            throw new Error ('Unexpected nft type')
                        }

                        let revokeResult = await revoke({action: action, tokenId: tokenId});
                        console.log("revokeResult", revokeResult);

                        sql = `delete from "${tableName}" where PK = '${mintingAsset.PK}' and SK = '${mintingAsset.SK}'`;
                        let deleteMintingAssetResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                        console.log("deleteMintingAssetResult", deleteMintingAssetResult);

                    }
                    else {
                        throw new Error("nft not belong to this wallet");    
                    }
                }
                else {
                    throw new Error("Failed to get nft owner");
                }
                
                sql = `update "${tableName}" set status = 'NEW', data = '', app_pub_key = '', token = '${token}', modified_date = '${new Date().toISOString()}' where PK = '${queueItem.PK}' and SK = '${queueItem.SK}' and modified_date = '${queueItem.modified_date}'`;
                let resetQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("resetQueueResult", resetQueueResult);

                sql = `update "${tableName}" set status = 'DONE' where PK = '${queueItem.PK}' and SK = '${queueItem.SK}' and status = 'PROCESSING'`;
                let resetDeQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("resetDeQueueResult", resetDeQueueResult);
            }
            else {
                sql = `update "${tableName}" set status = 'NEW', data = '', app_pub_key = '', token = '${token}', modified_date = '${new Date().toISOString()}' where PK = '${queueItem.PK}' and SK = '${queueItem.SK}' and modified_date = '${queueItem.modified_date}'`;

                let resetQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("resetQueueResult", resetQueueResult);
            }

            return {
                Success: true
            }
        }
        else {
            return {
                Success: false,
                Message: "QueueId not found: " + body.queueId
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-queue-reset-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-queue-reset-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-queue-reset-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key=='SNS_TOPIC_ERROR').value
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