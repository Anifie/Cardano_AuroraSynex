import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const client = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function ToAsset(a) {
    return {
        Name: a.name,
        Description: a.description,
        ThumbnailURL: a.asset_thumbnail_url,
        URL: a.asset_url,
        ContractAddress: a.contract_address,
        TokenId: a.token_id,
        Status: a.status,
        AssetId: a.asset_id,
        MaxSupply: a.max_supply,
        MetadataURL: a.metadata_url,
        NFTURL: a.nft_url,
        MediaType: a.media_type,
        RoyaltiesPercentage: a.royalties_percentage,
        Tags: a.tags,
        AuthorAddress: a.author_address,
        AssetType: a.asset_type,
        Category: a.category_name,
        Liked: a.liked_count,
        Store: a.store_id,
        StoreName: a.store_name,
        CoffeeClaimStatus: a.coffee_claim_status,
        CoffeeClaimDate: a.coffee_claim_date,
        Owner: {
            Address: a.owner_address,
            Quantity: a.owned_quantity
        },
        SellOrder: {
            Id: a.sell_order_id,
            CurrencyCode: a.currency_code,
            Price: a.price,
            Quantity: a.sell_quantity,
            SellerAddress: a.seller_address,
            Status: a.sell_status
        },
        Auction: {
            Id: a.auction_id,
            CurrencyCode: a.currency_code,
            StartBid: a.auction_start_bid,
            DurationInSeconds: a.auction_duration_seconds,
            StartTimeUTC: a.auction_start_time_UTC,
            MinIncrementalBid: a.auction_min_incremental_bid,
            IsPaused: a.auction_is_paused,
            HighestBid: a.auction_highest_bid,
            HighestBidderAddress: a.auction_highest_bidder_address,
            Quantity: a.auction_quantity,
            SellerAddress: a.seller_address,
            Status: a.auction_status
        }
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
             !headers['origin'].includes("anifie.com")) || 
             headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com")) {
            tableName = process.env.TABLE_NAME_TEST;
        }

        if (!body.pageSize) 
            body.pageSize = 10;
        
        let sql;

        if(body.walletAddress) {
            sql = `SELECT * FROM "${tableName}"."InvertedIndex" where type = 'ASSET' and SK = 'MEMBERWALLET#${body.walletAddress}'`;
        }
        else if(body.memberId) {
            let _sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBER_ID#${body.memberId}' AND type = 'MEMBER' AND begins_with("PK", 'MEMBER#')`;
            let memberResult = await client.send(new ExecuteStatementCommand({ Statement: _sql }));
            if (memberResult.Items.length === 0) {
                console.log("member not found: " + body.memberId);
                return {
                    Success: false,
                    Message: "member not found: " + body.memberId
                };
            }
            let member = memberResult.Items.map(item => unmarshall(item))[0];

            sql = `SELECT * FROM "${tableName}" where type = 'ASSET' and SK = 'MEMBERWALLET#${member.wallet_address}'`;
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

            sql = `SELECT * FROM "${tableName}" where type = 'ASSET' and SK = 'MEMBERWALLET#${discordInteraction.wallet_address}'`;
        }
        else {
            sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" where type = 'ASSET'`;
        }
        
        if(body.contractAddress) {
            sql+= ` and contract_address = '${body.contractAddress}'`;
        }

        if(body.tokenId) {
            sql+= ` and token_id = '${body.tokenId}'`;
        }

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

        // Execute the SQL query
        let nextToken = body.lastKey;
        let allAssets = [];
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
            const assets = assetResult.Items.map(unmarshall);
            allAssets.push(...assets);

            attempt++;

            if (!nextToken || allAssets.length >= body.pageSize) break;
        }

        const response = {
            Success: true,
            Data: {
                assets: allAssets.map(a => ToAsset(a)),
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
