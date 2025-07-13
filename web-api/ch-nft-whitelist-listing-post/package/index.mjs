import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function ToWhiteList(a) {
    return {
        WhitelistId: a.whitelist_id,
        MemberId: a.user_id,
        WalletAddress: a.wallet_address,
        DiscordId: a.discord_user_id,
        WhiteListType: a.whitelist_type,
        AwardClaimedCount: a.award_claimed_count,
        CreatedDate: a.created_date
    };
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

        let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
        configs = configResult.Items.map(item => unmarshall(item));
        console.log("configs", configs);
        
        // var token = headers['authorization'];
        // console.log("token", token);
        
        // if(!token)  {
        //     console.log('missing authorization token in headers');
        //     const response = {
        //             Success: false,
        //             Message: "Unauthorize user"
        //         };
        //     return response;
        // }
        
        // let memberId = null;
        
        // //verify token
        // try{
        //     const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key=='JWT_SECRET').value);
        //     console.log("decoded", decoded);
            
        //     memberId = decoded.PlayerId;
            
        //     if (Date.now() >= decoded.exp * 1000) {
        //         const response = {
        //             Success: false,
        //             Message: "Token expired"
        //         };
        //         return response;
        //     }
        // }catch(e){
        //     console.log("error verify token", e);
        //     const response = {
        //         Success: false,
        //         Message: "Invalid token."
        //     };
        //     return response;
        // }

        // let sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBER_ID#${memberId}' and type = 'MEMBER' and begins_with("PK", 'MEMBER#')`;
        // let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        // if(memberResult.Items.length == 0) {
        //     console.log("member not found: " + memberId);
        //     const response = {
        //         Success: false,
        //         Message: "member not found: " + memberId
        //     };
        //     return response;
        // }
        // let member = memberResult.Items.map(unmarshall)[0];

        // if(member.role !== 'ADMIN') {
        //     return {
        //         Success: false,
        //         Message: "Unauthorized access"
        //     };
        // }


        

        let sql = `select * from "${tableName}"."ByTypeCreatedDate" WHERE type = 'WHITELIST' `;
        
        if(body.whitelistId) {
            sql += ` and PK = 'WHITELIST#${body.whitelistId}' `;
        }

        if(body.whitelistType) {
            sql += ` and whitelist_type = '${body.whitelistType}' `;
        }

        if(body.memberId) {
            sql += ` and SK = 'MEMBER#${body.memberId}' `;
        }

        sql += ' order by created_date DESC'
        
        let whitelistResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(whitelistResult.Items.length == 0) {
            console.log("Whitelist not found");
            return {
                Success: false,
                Message: "Whitelist not found"
            };
        }

        let whitelists = whitelistResult.Items.map(unmarshall);
        
        let _whiteLists = whitelists.map(x => ToWhiteList(x));
        console.log("_whiteLists", _whiteLists);

        return {
                    Success: true,
                    Data: _whiteLists
                };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-whitelist-listing-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-whitelist-listing-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-whitelist-listing-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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