import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import { ulid } from 'ulid';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

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
        
        var token = headers['authorization'];
        console.log("token", token);
        
        if(!token)  {
            console.log('missing authorization token in headers');
            const response = {
                    Success: false,
                    Message: "Unauthorize user"
                };
            return response;
        }
        
        let memberId = null;
        
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
        let member = memberResult.Items.map(unmarshall)[0];

        if(member.role !== 'ADMIN') {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }


        // if(!body.walletAddress) {
        //     return {
        //         Success: false,
        //         Message: "walletAddress is required"
        //     };
        // }

        if(!body.memberId && !body.discordUserId && !body.walletAddress) {
            return {
                Success: false,
                Message: "Either memberId or discordUserId or walletAddress is required"
            };
        }

        if(!body.whiteListType) {
            return {
                Success: false,
                Message: "whiteListType is required"
            };
        }

        if(body.whiteListType !== 'WHITELIST_MEMBER_BRONZE_PALEBLUEDOT' 
            && body.whiteListType !== 'WHITELIST_MEMBER_BRONZE_METAGARAGE' 
            && body.whiteListType !== 'WHITELIST_MEMBER_SILVER_PALEBLUEDOT'
            && body.whiteListType !== 'WHITELIST_MEMBER_SILVER_METAGARAGE'
            && body.whiteListType !== 'WHITELIST_MEMBER_GOLD_PALEBLUEDOT'
            && body.whiteListType !== 'WHITELIST_MEMBER_GOLD_METAGARAGE'
            && body.whiteListType !== 'PALEBLUEDOT_ADDITIONAL_NFT'
            && !body.whiteListType.includes('PALEBLUEDOT_WINNER_')
        ) {
                return {
                    Success: false,
                    Message: "Invalid whiteListType"
                };
        }

        let whitelistId = ulid();

        if(body.discordUserId) {
            sql = `select * from "${tableName}"."InvertedIndex" where SK = '${body.discordUserId}' and type = 'DISCORD' and interaction_type = 'JOIN' and status = 'DONE'`;
            let discordResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(discordResult.Items.length == 0) {
                return {
                    Success: false,
                    Message: 'Discord user not found ' + body.discordUserId
                }
            }
            let discord = discordResult.Items.map(unmarshall)[0];
            body.memberId = discord.user_id;
        }
        else if(body.walletAddress) {
            sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBERWALLET#${body.walletAddress}' and type = 'MEMBER'`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(memberResult.Items.length == 0) {
                return {
                    Success: false,
                    Message: 'Member with wallet address not found ' + body.walletAddress
                }
            }
            let member = memberResult.Items.map(unmarshall)[0];
            body.memberId = member.user_id;
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

        if(member.discord_user_id == undefined) {
            return {
                Success: false,
                Message: 'Member dont have discord user id : ' + member.user_id
            }
        }

        if(member.wallet_address == undefined) {
            return {
                Success: false,
                Message: 'Member dont have wallet address : ' + member.user_id
            }
        }

        sql = `select * from "${tableName}"."InvertedIndex" where type = 'WHITELIST' and whitelist_type = '${body.whiteListType}' and SK = '${member.PK}'`;
        let existingWhiteListResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(existingWhiteListResult.Items.length > 0) {
            return {
                Success: false,
                Message: 'Member already part of the whitelist'
            }
        }

        if(body.whiteListType.includes('PALEBLUEDOT_WINNER_')) {
            sql = `select * from "${tableName}"."InvertedIndex" where type = 'WHITELIST' and SK = '${member.PK}'`;
            let existingWhiteListResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            let whitelists = existingWhiteListResult.Items.map(unmarshall);
            if(whitelists.find(x => x.whitelist_type.includes('PALEBLUEDOT_WINNER_'))) {
                return {
                    Success: false,
                    Message: 'Only 1 award can be assign to 1 member'
                }
            }

            sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'WHITELIST' and whitelist_type = '${body.whiteListType}'`;
            let existingWhiteList2Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(existingWhiteList2Result.Items.length > 0) {
                return {
                    Success: false,
                    Message: 'The winner whitelist is already been taken: ' + body.whiteListType
                }
            }
        }

        let txStatements = [];

        sql = `INSERT INTO "${tableName}" 
                VALUE { 
                'PK': 'WHITELIST#${whitelistId}', 
                'SK': '${member.PK}', 
                'type': 'WHITELIST', 
                'whitelist_id': '${whitelistId}', 
                'whitelist_type': '${body.whiteListType}', 
                'wallet_address': '${member.wallet_address}', 
                'user_id': '${member.user_id}', 
                'discord_user_id': '${member.discord_user_id}', 
                'created_date': '${new Date().toISOString()}'}`;
        txStatements.push({ "Statement": sql });

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("insert mint whitelist", dbTxResult);

        return {
                    Success: true
                };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-whitelist-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-whitelist-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-whitelist-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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