import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
// import jwt from 'jsonwebtoken';
// import axios from 'axios';
// import * as jose from 'jose';
// import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

function ToAsset(a) {
    return {
        Name: a.name,
        Description: a.description,
        ThumbnailURL: a.asset_thumbnail_url,
        URL: a.asset_url,
        PolicyId: a.policy_id,
        Unit: a.unit,
        AssetName: a.asset_name,
        Metadata: a.metadata ? JSON.parse(a.metadata) : undefined,
        // ContractAddress: a.contract_address,
        // TokenId: a.token_id,
        Status: a.status,
        AssetId: a.asset_id,
        MaxSupply: a.max_supply,
        MetadataURL: a.metadata_url,
        RoyaltiesPercentage: a.royalties_percentage,
        Tags: a.tags,
        AuthorAddress: a.author_address,
        AssetType: a.asset_type,
        Category: a.category_name,
        Liked: a.liked_count,
        Store: a.store_id,
        StoreName: a.store_name,
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
    
    console.log("asset listing get event", event);
    
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

        let configResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'`}));
        configs = configResult.Items.map(unmarshall);
        console.log("configs", configs);

        if(body.sortBy === undefined){
            const response = {
                Success: false,
                Message: 'sortBy is required',
            };
            return response;
        }
        
        if(body.status === undefined){
            const response = {
                Success: false,
                Message: 'status is required',
            };
            return response;
        }
        
        if(body.pageSize === undefined){
            const response = {
                Success: false,
                Message: 'pageSize is required',
            };
            return response;
        }
        
        var sql = `SELECT * FROM "${tableName}"`;
        
        if(body.status == 'NOTFORSALE') {
            
            if(body.walletAddress === undefined || body.walletAddress === null){
                const response = {
                    Success: false,
                    Message: 'walletAddress is required',
                };
                return response;
            }
            
            sql += `."InvertedIndex"`;
            
            sql += ` WHERE SK = 'MEMBERWALLET#${body.walletAddress}' AND type = 'ASSET' AND status = 'NOTFORSALE' `;
            
            sql += ` AND owned_quantity > 0`;   // ERC1155 will have record with 0 owned_quantity

            if(body.unit && body.unit != '')
                sql += ` AND unit = '${body.unit}'`;

            if(body.currencyCode && body.currencyCode != '')
                sql += ` AND currency_code = '${body.currencyCode.toUpperCase()}'`;
                
            if(body.walletAddress && body.walletAddress != '') {
                let owner;
                let ownerMemberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBERWALLET#${body.walletAddress}' and type = 'MEMBER'`}));
                if(ownerMemberResult.Items.length == 0) {
                    // let ownerMemberSmartAccountResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}"."${process.env.TABLE_NAME_GSI_BY_SMARTACCOUNT}" WHERE wallet_address_smartaccount = '${body.walletAddress}' and type = 'MEMBER'`}).promise();
                    // if(ownerMemberSmartAccountResult.Items.length > 0) {
                    //     owner = ownerMemberSmartAccountResult.Items.map(unmarshall)[0];
                    // }
                }
                else {
                    owner = ownerMemberResult.Items.map(unmarshall)[0];
                }

                if(!owner) {
                    console.log("Owner not found . Wallet address: " + body.walletAddress);
                    return {
                        Success: true,
                        Data: { 
                                assets: []
                            }
                    };
                    // throw new Error("Owner not found . Wallet address: " + body.walletAddress);
                }

                sql += ` AND owner_user_id = '${owner.user_id}'`;
            }
                
            if(body.searchString && body.searchString != '')
                sql += ` AND contains("name", '${body.searchString}')`;
                

            // exclude chat data NFT
            sql += ` AND store_id <> 'TOKYODOME_CHATDATA' AND store_id <> 'CHATDATA' `;

            sql += ` ORDER BY PK DESC`;
        }
        else
        {
            sql += `."ByTypeCreatedDate" WHERE 1 = 1`;
            
            if(body.unit && body.unit != '')
                sql += ` AND unit = '${body.unit}'`;
            
            if(body.status && body.status != '')
                sql += ` AND status = '${body.status}'`;

            if(body.storeId && body.storeId != '')
                sql += ` AND store_id = '${body.storeId}'`;
            
            if(body.MIMEType && body.MIMEType != '')
                sql += ` AND contains("media_type", '${body.MIMEType.toLowerCase()}')`;
                
            if(body.priceFrom && body.priceFrom != '')
                sql += ` AND price >= '${body.priceFrom}'`;
                
            if(body.priceTo && body.priceTo != '')
                sql += ` AND price <= '${body.priceTo}'`;
            
            if(body.currencyCode && body.currencyCode != '')
                sql += ` AND currency_code = '${body.currencyCode.toUpperCase()}'`;
                
            if(body.walletAddress && body.walletAddress != '') {
                let owner;
                let ownerMemberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBERWALLET#${body.walletAddress}' and type = 'MEMBER'`}));
                if(ownerMemberResult.Items.length == 0) {
                    // let ownerMemberSmartAccountResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}"."${process.env.TABLE_NAME_GSI_BY_SMARTACCOUNT}" WHERE wallet_address_smartaccount = '${body.walletAddress}' and type = 'MEMBER'`}).promise();
                    // if(ownerMemberSmartAccountResult.Items.length > 0) {
                    //     owner = ownerMemberSmartAccountResult.Items.map(unmarshall)[0];
                    // }
                }
                else {
                    owner = ownerMemberResult.Items.map(unmarshall)[0];
                }

                if(!owner) {
                    console.log("Owner not found . Wallet address: " + body.walletAddress);
                    return {
                        Success: true,
                        Data: { 
                                assets: []
                            }
                    };
                    // throw new Error("Owner not found . Wallet address: " + body.walletAddress);
                }

                sql += ` AND owner_user_id = '${owner.user_id}'`;
            }
                
            if(body.searchString && body.searchString != '')
                sql += ` AND contains("name", '${body.searchString}')`;
            
            if(body.highestBidderAddress && body.highestBidderAddress != '')
                sql += ` AND auction_highest_bidder_address = '${body.highestBidderAddress}'`;

            if(body.featured !== undefined && body.featured !== null)
                sql += ` AND featured = ${body.featured.toString()}`;
                
            if(body.storeId && body.storeId != '')
                sql += ` AND store_id = '${body.storeId.toUpperCase()}'`;

             // exclude chat data NFT
            sql += ` AND store_id <> 'TOKYODOME_CHATDATA'  AND store_id <> 'CHATDATA' `;
            
            switch (body.sortBy) {
                case 'NEWLY_CREATED':
                // case 'STORE_NEWLY_CREATED':
                    if(body.lastKey && body.lastKey != '')
                        sql += ` AND created_date < '${body.lastKey}'`;
                        
                    sql += ` ORDER BY created_date DESC`;
                    break;
                    
                // case 'PRICE_DESC':
                // case 'STORE_PRICE_DESC':
                //     if(body.lastKey && body.lastKey != '')
                //         sql += ` AND price_created_date < '${body.lastKey}'`;
                        
                //     sql += ` ORDER BY price_created_date DESC`;
                //     break;
                    
                // case 'PRICE_ASC':
                // case 'STORE_PRICE_ASC':
                //     if(body.lastKey && body.lastKey != '')
                //         sql += ` AND price_created_date > '${body.lastKey}'`;
                        
                //     sql += ` ORDER BY price_created_date ASC`;
                //     break;
                    
                // case 'FAVOURITE':
                // case 'STORE_FAVOURITE':
                //     if(body.lastKey && body.lastKey != '')
                //         sql += ` AND liked_count_created_date < '${body.lastKey}'`;
                        
                //     sql += ` ORDER BY liked_count_created_date DESC`;
                //     break;
                    
                default:
                    throw new Error("sortBy " + body.sortBy + " is not implemented");
            }
        }
        console.log("sql", sql);

        var nextToken = null;
        var allAssets = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var assetResult = null;
        while (attempt < maxAttempts) {
            assetResult = await dbClient.send(
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

        if(allAssets.length > 0) {
            let storeSKs = allAssets.map(x => "'" + x.store_id + "'").filter(onlyUnique);
            let storesResult = await dbClient.send(new ExecuteStatementCommand({
                                                            Statement: `SELECT * FROM "${tableName}" 
                                                                        WHERE begins_with("PK", 'STORE#') AND SK IN (${storeSKs.join(", ")})`,
                                                        }));

            if(storesResult.Items.length > 0) {
                let stores = storesResult.Items.map(unmarshall);
                console.log("stores", stores);
                // replace store_name in allAssets
                allAssets = allAssets.map(x => {
                                if(stores.filter(y => y.store_id == x.store_id).length > 0) {
                                    return {...x, store_name: stores.filter(y => y.store_id == x.store_id)[0].name}
                                }
                                else {
                                    return {...x, store_name: ""}
                                }
                            });
            }
            else {
                console.log("no store found. storeSKs : ", storeSKs);
            }
        }

        const response = {
            Success: true,
            Data: { 
                    assets: allAssets.map(a => ToAsset(a)), 
                    lastKey: assetResult.LastEvaluatedKey 
                }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-asset-listing-get ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-asset-listing-get - ' + random10DigitNumber,
            Message: `Error in ch-asset-listing-get: ${e.message}\n\nStack trace:\n${e.stack}`,
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