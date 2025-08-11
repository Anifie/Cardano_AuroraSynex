import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

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

        let token = headers['authorization'];
        console.log("token", token);

        let memberId = null;
        let member;
        let aggregateVerifier;

        if (body.appPubKey){
            
            if(!body.walletAddress){
                
                console.log("walletAddress is required");
                
                return {
                    Success: false,
                    Message: 'walletAddress is required',
                };
            }

            if(!token)  {
                console.log('missing authorization token in headers');
                const response = {
                        Success: false,
                        Code: 1,
                        Message: "Unauthorize user"
                    };
                return response;
            }
        
            //verify token
            try{
                const idToken = token.split(' ')[1] || "";
                const jwks = jose.createRemoteJWKSet(new URL("https://api.openlogin.com/jwks"));
                const jwtDecoded = await jose.jwtVerify(idToken, jwks, {
                                                                            algorithms: ["ES256"],
                                                                        });
                console.log("jwtDecoded", JSON.stringify(jwtDecoded));
        
                if ((jwtDecoded.payload).wallets[0].public_key == body.appPubKey) {
                    // Verified
                    console.log("Validation Success");
                } else {
                    // Verification failed
                    console.log("Validation Failed");
                    return {
                        Success: false,
                        Code: 1,
                        Message: "Validation failed"
                    };
                }
                
                memberId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier)
                console.log("memberId", memberId);
                
                aggregateVerifier = jwtDecoded.payload.aggregateVerifier;
                body.displayName = jwtDecoded.payload.name;
                
            }catch(e){
                console.log("error verify token", e);
                const response = {
                    Success: false,
                    Code: 1,
                    Message: "Invalid token."
                };
                return response;
            }

        } else {
            console.log("Invalid token.");
            return {
                Success: false,
                Code: 1,
                Message: "Invalid token."
            };
        }


        console.log("memberId", memberId);
            
        let sql = `SELECT * FROM "${tableName}" WHERE PK = 'MEMBER#${memberId}' AND type = 'MEMBER'`;
        let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if (memberResult.Items.length === 0) {

            console.log('inserting new member');

            let txStatements = [];

            sql = `INSERT INTO "${tableName}" 
                            VALUE {
                                'PK': '${'MEMBER#' + memberId}',
                                'SK': '${'MEMBERWALLET#' + body.walletAddress}',
                                'type': 'MEMBER',
                                'wallet_address': '${body.walletAddress}',
                                'biodata': '',
                                'banner_uri': '',
                                'email': '',
                                'phone': '',
                                'avatar_uri': 'https://i.pravatar.cc/150?u=${memberId}',
                                'user_id': '${memberId}',
                                'raw_data': '',
                                'roles': 'MEMBER',
                                'discord_roles': 'AURORA',
                                'display_name': '${body.displayName ? body.displayName : 'Anonymous'}',
                                'aggregate_verifier': '${aggregateVerifier}',
                                'consent_date': '${body.consentDate ? body.consentDate : ''}',
                                'created_date': '${new Date().toISOString()}'
                            }`;

            console.log(sql);

            txStatements.push({ "Statement": sql});

            sql = `insert into "${tableName}" value { 'PK': 'ROLE#MEMBER' , 'SK': 'MEMBER#${memberId}' , 'type': 'ROLE_MEMBER' , 'role_name': 'MEMBER', 'user_id': '${memberId}', 'wallet_address': '${body.walletAddress}', 'created_date': '${new Date().toISOString()}'}`
            txStatements.push({ "Statement": sql});

            const statements = { "TransactStatements": txStatements };  
            console.log("statements", JSON.stringify(statements));
            const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
            console.log("insert member tx dbResult", dbTxResult);

            // get from DB again
            const profileResult = await dbClient.send(new ExecuteStatementCommand({
                                                                Statement: `
                                                                    SELECT * FROM "${tableName}" WHERE PK = ?
                                                                `,
                                                                Parameters: [{ S: 'MEMBER#' + memberId }],
                                                            }));

            let _profile = ToMemberViewModel(profileResult.Items.map(unmarshall)[0]);

            const _response = {
                Success: true,
                Data: {
                    profile: _profile, 
                    stores: [],
                    favourites: [],
                    isFirstTimeSignIn: true
                }
            };
            
            console.log("_response", _response)
            
            return _response;
        }
        else {

            console.log('existing member found');

            member = memberResult.Items.map(unmarshall)[0];

            if(member.avatar_uri === '' || member.avatar_uri === undefined) {
                let sql = `update "${tableName}" set avatar_uri = 'https://i.pravatar.cc/150?u=${member.user_id}' where PK = '${member.PK}' and SK = '${member.SK}'`;
                let updAvatarResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}))
                console.log('updAvatarResult', updAvatarResult);
            }

            if((member.display_name == 'Anonymous' || !member.display_name) && body.displayName) {
                let sql = `update "${tableName}" set display_name = '${body.displayName}' where PK = '${member.PK}' and SK = '${member.SK}'`;
                let updDisplayNameResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}))
                console.log('updDisplayNameResult', updDisplayNameResult);
            }

            if(member.roles === undefined) {
                console.log("add the default role MEMBER");
                let sql = `update "${tableName}" set roles = 'MEMBER' where PK = '${member.PK}' and SK = '${member.SK}'`;
                let updRoleResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}))
                console.log('updRoleResult', updRoleResult);

                sql = `insert into "${tableName}" value { 'PK': 'ROLE#MEMBER' , 'SK': '${member.PK}' , 'type': 'ROLE_MEMBER' , 'role_name': 'MEMBER', 'user_id': '${member.user_id}', 'wallet_address': '${member.wallet_address}', 'created_date': '${new Date().toISOString()}'}`
                let insertRoleMemberResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}))
                console.log('insertRoleMemberResult', insertRoleMemberResult);
            }

            let discordUser;

            if(member.discord_user_id) { 

                // get discord global name
                try {
                        
                    const GUILD_ID = configs.find(x => x.key == 'DISCORD_GUILD_ID').value;
                    const BOT_TOKEN = configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value;

                    let url = `https://discord.com/api/v10/guilds/${GUILD_ID}/members/${member.discord_user_id}`
                    console.log('get discord user url', url);
                    let _headers = {
                                        "Authorization": `Bot ${BOT_TOKEN}`,
                                        "Content-Type": "application/json"
                                    };
                    let userResult = await axios.get(url,
                                                        {
                                                            headers: _headers,
                                                        });
                    console.log("get discord user result", userResult);
        
                    discordUser = userResult.data;
                    
                } catch (err) {
                    console.log(err);
                    const _message = {
                        Subject: 'Honda Error - ch-member-signin',
                        Message: "failed to get discord user " + member.discord_user_id,
                        TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                    };
                    await snsClient.send(new PublishCommand(_message));
                }

                if(discordUser && discordUser.user.global_name && discordUser.user.global_name != member.display_name) {
                    let sql = `update "${tableName}" set display_name = '${discordUser.user.global_name}' where PK = '${member.PK}' and SK = '${member.SK}'`;
                    let updateDisplayNameResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log("syncDiscordNameToWidgetBotResult", updateDisplayNameResult);
                }

            }


            let _profile = ToMemberViewModel(member);

            const _response = {
                Success: true,
                Data: {
                    profile: _profile, 
                    // stores: memberStores.Items.map(unmarshall).map(x => ToStoreViewModel(x)),
                    // favourites: memberFavourites.Items.map(unmarshall).map(x => ToFavouriteViewModel(x)),
                    // // wonAuctionCount: _wonAuctionCount,
                    // announcements: unseen_announcements.map(x=>ToAnnouncementViewModel(x)),
                    isFirstTimeSignIn: false,
                }
            };
            
            console.log("_response", _response)
            
            return _response;
        }

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-member-signin ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-member-signin - ' + random10DigitNumber,
            Message: `Error in ch-member-signin  ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
    
        return {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
    }
};