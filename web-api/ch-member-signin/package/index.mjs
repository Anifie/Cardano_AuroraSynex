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

            ...

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

            ...

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