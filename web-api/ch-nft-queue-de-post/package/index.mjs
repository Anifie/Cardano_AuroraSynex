import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 10 * 60 * 1000 // 10 minutes in milliseconds
    }
});

let tableName;
let configs;

const discordMessage = async (params, origin) => {
    console.log("discordMessage", params);
    let response = await axios.post(configs.find(x => x.key == 'API_URL').value + '/discord/message',
                                        JSON.stringify(params),
                                        {
                                            headers: {
                                                'Content-Type': 'application/json',
                                                'origin': origin
                                            }
                                        }
                                    );
    console.log(response.data);
    console.log('discordMessage jsonResult', response.data);
    return response.data;
}

export const handler = async (event) => {
    console.log("nft dequeue event", event);
    
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

        
        if(!body.queueType) {
            console.log("queueType is required");
            return {
                Success: false,
                Message: 'queueType is required'
            };
        }

        let sk;
        switch(body.queueType) {
            case "MINT_QUEUE":
                sk = 'QUEUE_STATUS';
                break;
            case "UPGRADE_QUEUE":
                sk = "UPGRADE_QUEUE_STATUS";
                break;
            case "UPDATE_QUEUE":
                sk = "UPDATE_QUEUE_STATUS";
                break;
        }

        let sql = `select * from "${tableName}" where PK = 'ENUM' and SK = '${sk}'`;
        // console.log(sql);
        let queueStatusEnumResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        let queueStatusEnum = queueStatusEnumResult.Items.map(unmarshall)[0];
        if(queueStatusEnum.enum_values == 'PROCESSING') {
            console.log("Processing queue in progress");
            return {
                Success: false,
                Message: 'Processing queue in progress'
            };
        }
        console.log("queueStatusEnum", queueStatusEnum);

        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'QUEUE' and status = 'NEW' and queue_type = '${body.queueType}' order by created_date desc`;
        console.log(sql);
        let queueItemsResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(queueItemsResult.Items.length > 0) {
            
            console.log("Update queue status to processing");
            sql = `update "${tableName}" set enum_values = 'PROCESSING' where PK = 'ENUM' and SK = '${sk}' and enum_values = 'DONE'`;
            console.log(sql);
            let updateQueueStatusResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
            console.log('updateQueueStatusResult to processing', updateQueueStatusResult);

            let isFailed = false;

            let queueItems = queueItemsResult.Items.map(unmarshall);
            
            // queueItems = queueItems.splice(0,1);    // process only 1 item at a time.
            
            const item = queueItems.pop();

            // // each lambda execution take around 2 minutes, so we update all queue item to in progress first , to book them
            // for (let i = 0; i < queueItems.length; i++) {
            //     const item = queueItems[i];

                console.log("item", item);

                let _sql;

                if(item.modified_date) {
                    _sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , status = 'IN_PROGRESS' where PK = '${item.PK}' and SK = '${item.SK}' and modified_date = '${item.modified_date}'`;
                }
                else {
                    _sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , status = 'IN_PROGRESS' where PK = '${item.PK}' and SK = '${item.SK}' and modified_date is missing`;
                }

                let updateQueueInProgressResult = await dbClient.send(new ExecuteStatementCommand({Statement: _sql}));
                console.log("updateQueueInProgressResult", updateQueueInProgressResult);
            // }

            // for (let i = 0; i < queueItems.length; i++) {
            //     const item = queueItems[i];
                
                let lambdaParams;
                let payload;
                let successMessage;

                if(body.queueType == 'MINT_QUEUE') {
                    lambdaParams = {
                        FunctionName: 'ch-nft-mint-post',
                        InvocationType: 'RequestResponse', 
                        LogType: 'Tail',
                        Payload: {
                            body: JSON.stringify({
                                appPubKey: item.app_pub_key === '' || item.app_pub_key == undefined ? undefined : item.app_pub_key,
                                nftType: item.nft_type,
                                artworkId: item.artwork_id,
                                artworkId2: item.artwork_id_2,
                                artworkId3: item.artwork_id_3,
                                queueId: item.queue_id,
                                memberId: item.user_id == '01GJ5XT15FHWPFRN5QJSPXKW0X' ? 'LOCAL#admin@ada.com' : item.user_id,
                                forcePreregister: item.force_pregister,
                                forceRegular: item.force_regular,
                                forceRacingFan1: item.force_racing_fan_1,
                                forceRacingFan2: item.force_racing_fan_2,
                                forceRacingFan3: item.force_racing_fan_3
                            }),
                            headers: {
                                origin: headers['origin'],
                                authorization: item.token
                            }
                        }
                    };
                    lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                    console.log("lambdaParams", lambdaParams);     

                    // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);
                    console.log("lambdaParams", lambdaParams);
                    let lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
                    console.log("lambdaResult", lambdaResult); 
                    payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
                    console.log("payload", payload);

                    if(payload.Success) {
                        sql = `select * from "${tableName}" where type = 'ASSET' and PK = 'ASSET#${payload.Data.unit}'`;
                        console.log("sql", sql);
                        let assetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        let asset = assetResult.Items.map(unmarshall)[0];
                        // let domain = (tableName == process.env.TABLE_NAME ? process.env.HOMEPAGE_URL : process.env.HOMEPAGE_URL_TEST);
                        let domain = configs.find(x => x.key == 'HOMEPAGE_URL').value;
                        successMessage = `<@${item.discord_user_id}> ` + asset.name +  `のミントが完了しました。` + domain + "/assets";
                    }
                }
                else if(body.queueType == 'UPGRADE_QUEUE') {
                    lambdaParams = {
                        FunctionName: 'ch-nft-upgrade-post',
                        InvocationType: 'RequestResponse', 
                        LogType: 'Tail',
                        Payload: {
                            body: JSON.stringify({
                                appPubKey: item.app_pub_key === '' || item.app_pub_key == undefined ? undefined : item.app_pub_key,
                                unit: item.unit,
                                queueId: item.queue_id
                            }),
                            headers: {
                                origin: headers['origin'],
                                authorization: item.token
                            }
                        }
                    };
                    lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                    console.log("lambdaParams", lambdaParams);     
                    let lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
                    console.log("lambdaResult", lambdaResult); 
                    payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
                    console.log("payload", payload);
                    
                    if(payload.Success) {
                        sql = `select * from "${tableName}" where type = 'ASSET' and PK = 'ASSET#${item.unit}'`;
                        console.log("sql", sql);
                        let assetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        let asset = assetResult.Items.map(unmarshall)[0];
                        let domain = configs.find(x => x.key == 'HOMEPAGE_URL').value;
                        successMessage = `<@${item.discord_user_id}> ` + asset.name +  `のリビールが完了しました。` + domain + "/assets";
                    }
                }
                else if(body.queueType == 'UPDATE_QUEUE') {
                    lambdaParams = {
                        FunctionName: 'ch-nft-update-post',
                        InvocationType: 'RequestResponse', 
                        LogType: 'Tail',
                        Payload: {
                            body: JSON.stringify({
                                appPubKey: item.app_pub_key === '' || item.app_pub_key == undefined ? undefined : item.app_pub_key,
                                nftType: item.nft_type,
                                unit: item.unit,
                                queueId: item.queue_id,
                                artworkId: item.artwork_id,
                                artworkIdV2: item.artwork_id_2,
                                messages: item.messages ? (typeof item.messages == 'object' ? item.messages : JSON.parse(item.messages)) : undefined,
                                chatMemberId: item.chat_member_id,
                                metadata: item.metadata ? (typeof item.metadata == 'object' ? item.metadata : JSON.parse(item.metadata)) : undefined
                            }),
                            headers: {
                                origin: headers['origin'],
                                authorization: item.token
                            }
                        }
                    };
                    lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                    console.log("lambdaParams", lambdaParams);     
                    let lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
                    console.log("lambdaResult", lambdaResult); 
                    payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
                    console.log("payload", payload);
                    
                    if(payload.Success && !item.metadata) {
                        sql = `select * from "${tableName}" where type = 'ASSET' and PK = 'ASSET#${item.unit}'`;
                        console.log("sql", sql);
                        let assetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        let asset = assetResult.Items.map(unmarshall)[0];
                        let domain = configs.find(x => x.key == 'HOMEPAGE_URL').value;
                        successMessage = `<@${item.discord_user_id}> ` + asset.name +  `のリビールが完了しました。` + domain + "/assets";
                    }
                }
                
                if(payload.Success) {
                    sql = `update "${tableName}" set status = 'SUCCESS', data = '${JSON.stringify(payload.Data)}', modified_date = '${new Date().toISOString()}' where PK = '${item.PK}' and SK = '${item.SK}'`;
                    console.log(sql);
                    updateQueueStatusResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log('updateQueueStatusResult to SUCCESS', updateQueueStatusResult);

                    // send discord direct message to user
                    try {
                        if(successMessage) {
                            let messageResult = await discordMessage({
                                discordUserId: item.discord_user_id,
                                message: successMessage,
                                passcode: configs.find(x => x.key == 'PASSCODE').value,
                            },
                            headers['origin'])    
                            console.log("discordMessageResult", messageResult);
                        }   
                    } catch (error) {
                        console.log('Failed to send message to user about mint');
                        const msg = {
                            Subject: 'Honda Cardano Error - ch-nft-queue-de-post',
                            Message: 'Failed to send message to user about mint. QueueId: ' + item.queue_id + ' . MemberId: ' + item.user_id,
                            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                        };
                        await snsClient.send(new PublishCommand(msg));
                    }
                }
                else {
                    sql = `update "${tableName}" set status = 'FAILED', data = '${payload.Message}', modified_date = '${new Date().toISOString()}' where PK = '${item.PK}' and SK = '${item.SK}'`;
                    console.log(sql);
                    updateQueueStatusResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log('updateQueueStatusResult to FAILED', updateQueueStatusResult);

                    // stop if unhandled exception
                    if(payload.Message.includes('。Code: ') || payload.Message.includes('失敗') || payload.Message.includes('fail') || payload.Message.includes('Fail') ) {
                        isFailed = true;
                        // break;
                    }
                }

            // }

            if(!isFailed) {
                console.log("Update queue status to done");
                sql = `update "${tableName}" set enum_values = 'DONE' where PK = 'ENUM' and SK = '${sk}' and enum_values = 'PROCESSING'`;
                updateQueueStatusResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                console.log('updateQueueStatusResult to done', updateQueueStatusResult);            
            }
            else {
                throw new Error('NFT Queue processor failed. ' + body.queueType);
            }
        }

        return {
            Success: true
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-queue-de-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-queue-de-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-queue-de-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: e.message
            //Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};