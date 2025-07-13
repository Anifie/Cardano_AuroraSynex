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
        requestTimeout: 1 * 60 * 1000 // 1 minutes in milliseconds
    }
});

let tableName;
let configs;

export const handler = async (event) => {
    
    console.log("nft owner check event", event);
    
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

        if(!body.nftType) {
            return {
                Success: false,
                Message: 'nftType is required'
            }
        }

        if(body.nftType != 'MEMBER_A' && body.nftType != 'MEMBER_B') {
            return {
                Success: false,
                Message: 'Only MEMBER_A and MEMBER_B nftType is supported'
            }
        }
        
        if(!body.unit) {
            return {
                Success: false,
                Message: 'unit is required'
            }
        }
        
        let lambdaParams = {
            FunctionName: 'ch-web3',
            InvocationType: 'RequestResponse', 
            LogType: 'Tail',
            Payload: {
                action: 'MEMBERSHIP_OWNER',
                unit: body.unit,
                isTest: tableName == process.env.TABLE_NAME_TEST
            }
        };
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
        console.log("lambdaParams", lambdaParams);      
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        console.log("lambdaResult", lambdaResult);
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        console.log("payload", payload);
        if (payload.errorMessage) {
            console.error("lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('Lambda error: ' + JSON.stringify(payload.errorMessage));
        }
        let ownerResult = payload;

        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
        // console.log("lambdaParams", lambdaParams);            
        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
        // console.log("lambdaResult", lambdaResult);            
        // if(lambdaResult.Payload.errorMessage) {
        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
        // }            
        // let ownerResult = JSON.parse(lambdaResult.Payload);    

        console.log("ownerResult", ownerResult);        
        if(ownerResult != undefined) {
            console.log("ownerResult nft. owner: "  + ownerResult);

            return {
                Success: true,
                Data: {
                    owner: ownerResult
                }
            }
        }
        else {
            console.log('Failed to get NFT owner in blockchain');
            return {
                Success: false,
                Message: "Failed to get NFT owner in blockchain"
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-ownerof-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-ownerof-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-ownerof-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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