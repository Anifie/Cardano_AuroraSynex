import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
//import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
// const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function ToEnumViewModel(obj){
    return {
        enum_name: obj.enum_name,
        enum_values: obj.enum_values.split(','),
        enum_description: obj.enum_description ? obj.enum_description.split(',') : undefined
    }
}

export const handler = async (event) => {
    
    console.log("event", event);

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

        // let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
        // configs = configResult.Items.map(item => unmarshall(item));
        // console.log("configs", configs);

        let sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM'`;
        let enumResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        let enums = enumResult.Items.map(item => unmarshall(item));

        console.log("enums", enums);

        return {
            Success: true,
            Data: enums.map(x => ToEnumViewModel(x))
        };

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-enum-get ' + random10DigitNumber, e);
    
        // const message = {
        //     Subject: 'Honda Cardano Error - td-enum-get  ' + random10DigitNumber,
        //     Message: `Error in td-enum-get ${e.message}\n\nStack trace:\n${e.stack}`,
        //     TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
        // };
        
        // if(tableName == process.env.TABLE_NAME)
        //     await snsClient.send(new PublishCommand(message));
    
        return {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
    }

    var result = await db.executeStatement({
                                        Statement: `SELECT * FROM "${process.env.TABLE_NAME_RIPPLE_METAVERSE}" WHERE PK = ?`,
                                        Parameters: [{ S: 'ENUM'}]
                                    }).promise();
    
    const response = {
            Success: true,
            Code: 1,
            Data: result.Items.map(unmarshall).map(x=>ToEnumViewModel(x))
        };
        
    return response;
};
