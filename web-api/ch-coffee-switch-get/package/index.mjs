import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

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

        let enumResult = await dbClient.send(new ExecuteStatementCommand({
            Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' AND SK = 'COFFEE' `
        }));
        if (enumResult.Items.length === 0) {
            return {
                Success: false,
                Message: "Coffee setting not found"
            };
        }
        let _enum = enumResult.Items.map(item => unmarshall(item))[0];
        console.log("_enum", _enum);
        
        return {
            Success: true,
            Data: _enum.enum_values
        };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-coffee-switch-get ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-coffee-switch-get - ' + random10DigitNumber,
            Message: `Error in ch-coffee-switch-get: ${e.message}\n\nStack trace:\n${e.stack}`,
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