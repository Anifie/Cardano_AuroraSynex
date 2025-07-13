import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
// import jwt from 'jsonwebtoken';
// import axios from 'axios';
// import * as jose from 'jose';
// import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

async function fetchAllRecords(sql) {
    let results = [];
    let nextToken;

    do {
        const params = {
            Statement: sql,
            NextToken: nextToken, // Include NextToken if available
        };

        const result = await dbClient.send(new ExecuteStatementCommand(params));

        // Accumulate items from this page
        if (result.Items) {
            results = results.concat(result.Items);
        }

        // Update nextToken for the next iteration
        nextToken = result.NextToken;
    } while (nextToken); // Continue until there's no nextToken

    return results;
}

function ToMemberViewModel(obj){
    return {
        avatar_uri: obj.avatar_uri ? obj.avatar_uri : `https://i.pravatar.cc/150?u=${obj.user_id}`,
        banner_uri: obj.banner_uri,
        biodata: obj.biodata,
        email: obj.email,
        phone: obj.phone,
        wallet_address: obj.wallet_address,
        // wallet_address_smartaccount: obj.wallet_address_smartaccount,
        user_id: obj.user_id,
        display_name: obj.display_name ? obj.display_name : 'Anonymous',
        is_consent: obj.is_consent === undefined ? false : obj.is_consent,
        survey_completed: obj.survey_completed,
        discord_user_id: obj.discord_user_id,
        // discord_user_id_real: obj.discord_user_id_real,
        isAMember: obj.nft_member_a_asset_name != undefined,
        isBMember: obj.nft_member_b_asset_name != undefined,
        created_date: obj.created_date,
        campaign_code: obj.campaign_code,
        campaign_code_project: obj.campaign_code_project,
        consent_date: obj.consent_date,
        xp_total: obj.xp_total,
        xp_level: obj.xp_level,
        settings: obj.settings,

        nft_member_a_asset_name: obj.nft_member_a_asset_name,
        nft_member_a_policy_id: obj.nft_member_a_policy_id,

        nft_member_b_asset_name: obj.nft_member_b_asset_name,
        nft_member_b_policy_id: obj.nft_member_b_policy_id,

        nft_racingfan_asset_name: obj.nft_racingfan_asset_name,
        nft_racingfan_policy_id: obj.nft_racingfan_policy_id,
    }
}

export const handler = async (event) => {
    
    console.log("admin member listing get event", event);
    
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

        if(body.pageSize === undefined){
            const response = {
                Success: false,
                Message: 'pageSize is required',
            };
            return response;
        }

        let sql;

        if(body.discordUserId !== undefined){
            sql = `select * from "${tableName}"."ByTypeCreatedDate" where discord_user_id = '${body.discordUserId}' and type = 'MEMBER'`;
            const _memberResult = await fetchAllRecords(sql);
            if(_memberResult.length > 0) {
                let _member = _memberResult.map(unmarshall)[0];
                
                const response = {
                    Success: true,
                    Data: { 
                            members: [ToMemberViewModel(_member)],
                            lastKey: undefined
                        }
                };
                
                return response;
                
            }
        }

        if(body.discordUserIdReal !== undefined){
            sql = `select * from "${tableName}"."ByTypeCreatedDate" where discord_user_id_real = '${body.discordUserIdReal}' and type = 'MEMBER'`;
            const _memberResult = await fetchAllRecords(sql);
            if(_memberResult.length > 0) {
                let _member = _memberResult.map(unmarshall)[0];
                
                const response = {
                    Success: true,
                    Data: { 
                            members: [ToMemberViewModel(_member)],
                            lastKey: undefined
                        }
                };
                
                return response;
                
            }
        }

        if(body.walletAddress !== undefined){
            sql = `SELECT * FROM "${tableName}"."InvertedIndex" where SK = 'MEMBERWALLET#${body.walletAddress}' and type = 'MEMBER'`;
        }
        else if(body.memberId !== undefined){
            sql = `SELECT * FROM "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
        }
        else {
            sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate"`;
            sql += ` WHERE type = 'MEMBER'`;

            // if(body.memberDisplayName !== undefined){
            //     sql += ` AND contains("display_name" , '${body.memberDisplayName}')`;
            // }

            // if(body.smartWalletAddress !== undefined){
            //     sql += ` AND wallet_address_smartaccount = '${body.smartWalletAddress}'`;
            // }

            if(body.memberId !== undefined){
                sql += ` AND user_id = '${body.memberId}'`;
            }

            if(body.lastKey && body.lastKey != '')
                sql += ` AND created_date < '${body.lastKey}'`;

            sql += ` AND role <> 'ADMIN'  ORDER BY created_date DESC`;
        }

        console.log("sql", sql);

        var nextToken = null;
        var allMembers = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var memberResult = null;
        while (attempt < maxAttempts) {
            memberResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = memberResult.NextToken;
            const members = memberResult.Items.map(unmarshall);
            allMembers.push(...members);

            attempt++;

            if (!nextToken || allMembers.length >= body.pageSize) break;
        }
        
        const response = {
            Success: true,
            Data: { 
                    members: allMembers.map(a => ToMemberViewModel(a)), 
                    lastKey: memberResult.LastEvaluatedKey 
                }
        };
        
        return response;
        
    } catch (e) {
        console.error('error in ch-admin-member-listing-get', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};
