import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const client = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function ToMember(obj) {
    return {
        IsAMember: obj.nft_member_a_token_id != undefined,
        IsBMember: obj.nft_member_b_token_id != undefined,
        MemberId: obj.user_id,
        DiscordId: obj.discord_user_id,
        WalletAddress: obj.wallet_address,
        CoffeeClaimStatus: obj.coffee_claim_status,
        CoffeeClaimDate: obj.coffee_claim_date
    };
}

export const handler = async (event) => {
    console.log("coffee listing get event", event);
    
    let tableName;
    try {
        const headers = event.headers;
        const body = JSON.parse(event.body);

        tableName = process.env.TABLE_NAME;
        if ((!headers['origin'].includes("anifie.community.admin") && 
             !headers['origin'].includes("honda-synergy-lab.jp") && 
             !headers['origin'].includes("anifie.com") && 
             !headers['origin'].includes("global.honda")) || 
             headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com")) {
            tableName = process.env.TABLE_NAME_TEST;
        }

        if (!body.pageSize) 
            body.pageSize = 10;
        
        let sql;

        if(body.walletAddress) {
            sql = `SELECT * FROM "${tableName}"."InvertedIndex" where type = 'MEMBER' and SK = 'MEMBERWALLET#${body.walletAddress}' `;
        }
        else if(body.memberId) {
            sql = `SELECT * FROM "${tableName}" WHERE PK = 'MEMBER#${body.memberId}' AND type = 'MEMBER' `;
        }
        else if(body.discordId) {
            let _sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${body.discordId}' AND type = 'DISCORD' and interaction_type = 'JOIN'`;
            let discordResult = await client.send(new ExecuteStatementCommand({ Statement: _sql }));
            if (discordResult.Items.length === 0) {
                console.log("discord not found: " + body.discordId);
                return {
                    Success: false,
                    Message: "discord not found: " + body.discordId
                };
            }
            let discordInteraction = discordResult.Items.map(item => unmarshall(item))[0];

            sql = `SELECT * FROM "${tableName}"."InvertedIndex" where type = 'MEMBER' and SK = 'MEMBERWALLET#${discordInteraction.wallet_address}' `;
        }
        else {
            sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" where type = 'MEMBER'`;
        }
        
        // if(body.contractAddress) {
        //     sql+= ` and contract_address = '${body.contractAddress}'`;
        // }

        // if(body.tokenId) {
        //     sql+= ` and token_id = '${body.tokenId}'`;
        // }

        if(body.coffeeClaimStatus == 'CLAIMED') {
            sql+= ` and coffee_claim_status = '${body.coffeeClaimStatus}'`;
        }
        else if(body.coffeeClaimStatus == 'UNCLAIM') {
            sql+= ` and coffee_claim_status <> 'CLAIMED'`;
        }

        // if(!body.walletAddress && !body.memberId && !body.discordId && body.lastKey && body.lastKey !== '') {
        //     sql += ` AND created_date < '${body.lastKey}'`;
        // }

        if(!body.walletAddress && !body.memberId && !body.discordId)
            sql += ` order by created_date DESC`;

        console.log("sql", sql);
        
        // Execute the SQL query
        let nextToken = body.lastKey;
        let allMembers = [];
        const maxAttempts = 40;
        let attempt = 0;

        while (attempt < maxAttempts) {
            const assetResult = await client.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = assetResult.NextToken;
            const members = assetResult.Items.map(unmarshall);
            allMembers.push(...members);

            attempt++;

            if (!nextToken || allMembers.length >= body.pageSize) break;
        }

        const response = {
            Success: true,
            Data: {
                members: allMembers.map(a => ToMember(a)),
                lastKey: nextToken
            }
        };
        return response;

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-coffee-listing-get ' + random10DigitNumber, e);

        const message = {
            Subject: `Honda Cardano Error - ch-coffee-listing-get - ${random10DigitNumber}`,
            Message: `Error in ch-coffee-listing-get: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };

        if (tableName === process.env.TABLE_NAME) {
            await snsClient.send(new PublishCommand(message));
        }

        return {
            Success: false,
            Message: `エラーが発生しました。管理者に連絡してください。Code: ${random10DigitNumber}`
        };
    }
};
