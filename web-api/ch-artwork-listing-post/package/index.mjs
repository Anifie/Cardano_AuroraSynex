import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

let MEMBERS = [];

async function ToArtworkViewModel(obj, tableName){

    let artwork = {
        Name: obj.name,
        Description: obj.description,
        ArtworkId: obj.artwork_id,
        MemberId: obj.user_id,
        ArtworkType: obj.artwork_type,
        Category: obj.category,
        SubCategory: obj.sub_category,
        Components: obj.components ? JSON.parse(obj.components) : undefined,
        NameEN: obj.name_en,
        ValueEN: obj.value_en,
        NameJP: obj.name_jp,
        ValueJP: obj.value_jp,
        Metadata: obj.metadata ? JSON.parse(obj.metadata) : undefined,
        Status: obj.status,
        TwoDURL: obj.two_d_url,
        TwoDMIME: obj.two_d_mime,
        TwoDURL_2: obj.two_d_url_2,
        TwoDMIME_2: obj.two_d_mime_2,
        TwoDURL_3: obj.two_d_url_3,
        TwoDMIME_3: obj.two_d_mime_3,
        ThreeDURL: obj.three_d_url,
        ThreeDMIME: obj.three_d_mime,
        CreatedDate: obj.created_date,
        LikedCount: obj.liked_count,
        Ranking: obj.ranking,
        VideoURL: obj.video_url,
        VideoMIME: obj.video_mime,
        Location: obj.location,
        Position: obj.position,
        Amount: obj.amount,
        Currency: obj.currency,
        DurationInMinutes: obj.duration_in_minutes,
        Expiry: obj.expiry,
        ReactionSummary: obj.reaction_summary ? JSON.parse(obj.reaction_summary) : undefined,
        Prompt: obj.prompt
    }

    if(obj.SK.includes("MEMBERWALLET#")) {
        
        let walletAddress = obj.SK.split("#")[1];

        let _member = MEMBERS.find(x => x.wallet_address == walletAddress);

        if(_member) {
            artwork.Member = _member;
        }
        else {
            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${obj.SK}' and type = 'MEMBER'`}));
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            if(memberResult.Items.length == 0) {
                throw Error ("Missing Member")
            }
            let member = memberResult.Items.map(unmarshall)[0];
            console.log("member", member);

            _member = ToMemberViewModel(member);
            MEMBERS.push(_member);

            artwork.Member = _member;
        }
    }

    return artwork;
    
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
    
    console.log("artwork listing get event", event);
    
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


        let token = headers['authorization'];
        console.log("token", token);

        let memberId = null;
        let member;

        if(!body.appPubKey && token) {
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
            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + memberId);
                const response = {
                    Success: false,
                    Message: "member not found: " + memberId
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];

            if(!member.role?.includes('ADMIN')) {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }

            // if(body.artworkType === 'FULL_USER') {
            //     // replace member with member who we want the artwork goes to
            //     if(body.memberId == undefined) {
            //         return {
            //             Success: false,
            //             Message: "memberId is required"
            //         };
            //     }
            //     sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            //     memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            //     if(memberResult.Items.length == 0) {
            //         console.log("member not found: " + body.memberId);
            //         const response = {
            //             Success: false,
            //             Message: "member not found: " + body.memberId
            //         };
            //         return response;
            //     }
            //     member = memberResult.Items.map(unmarshall)[0];
            // }
        }
        else if(body.appPubKey) {

            if(!token)  {
                console.log('missing authorization token in headers');
                const response = {
                        Success: false,
                        Code: 1,
                        Message: "Unauthorize user"
                    };
                return response;
            }

            let userId;
            // let aggregateVerifier;
        
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
                
                userId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier)
                console.log("userId", userId);
                
                // aggregateVerifier = jwtDecoded.payload.aggregateVerifier;
                
            }catch(e){
                console.log("error verify token", e);
                const response = {
                    Success: false,
                    Code: 1,
                    Message: "Invalid token."
                };
                return response;
            }

            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}));
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        // else {
        //     console.log('Missing login info');
        //     const response = {
        //             Success: false,
        //             Message: "Missing login info"
        //         };
        //     return response;
        // }



        if(body.pageSize === undefined){
            const response = {
                Success: false,
                Message: 'pageSize is required',
            };
            return response;
        }
    
        let sql;
        if(member && !member.role?.includes('ADMIN') && body.memberId !== undefined) {
            sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE type = 'ARTWORK' and SK = 'MEMBERWALLET#${member.wallet_address}' `;
        }
        else if (body.category) {
            sql = `SELECT * FROM "${tableName}"."ByCategory" WHERE type = 'ARTWORK' `;
        }
        else {
            sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE type = 'ARTWORK' `;
        }
        
        // if(body.memberDisplayName !== undefined){
        //     sql += ` AND contains("display_name" , '${body.memberDisplayName}')`;
        // }

        if(body.artworkId !== undefined){
            sql += ` AND artwork_id = '${body.artworkId}'`;
        }

        if(body.artworkType !== undefined){
            sql += ` AND artwork_type = '${body.artworkType}'`;
        }

        if(body.memberId !== undefined){
            sql += ` AND user_id = '${body.memberId}'`;
        }

        if(body.category !== undefined){
            sql += ` AND category = '${body.category}'`;
        }

        if(body.subCategory !== undefined){
            sql += ` AND sub_category = '${body.subCategory}'`;
        }

        if(body.status !== undefined){
            sql += ` AND status = '${body.status}'`;
        }

        if(body.location !== undefined){
            sql += ` AND location = '${body.location}'`;
        }

        if(body.position !== undefined){
            sql += ` AND position = '${body.position}'`;
        }

        if(body.afterDateTime !== undefined){
            sql += ` AND created_date > '${body.afterDateTime}'`;
        }

        if(member && !member.role?.includes('ADMIN') && body.memberId !== undefined) {
            sql += ` ORDER BY PK DESC`;
        }
        else if (body.category) {
            if(member.role?.includes('ADMIN')) {
                sql += ` ORDER BY created_date DESC`;
            }
            else {
                const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
                console.log(oneHourAgo);
                sql += ` AND created_date > '${oneHourAgo}' ORDER BY created_date DESC`;
            }
        }
        else {
            sql += ` ORDER BY created_date DESC`;
        }

        console.log("sql", sql);

        var nextToken = body.nextToken;
        var allArtworks = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var artworkResult = null;

        while (attempt < maxAttempts) {
            artworkResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = artworkResult.NextToken;
            const _artworks = artworkResult.Items.map(unmarshall);
            allArtworks.push(..._artworks);

            attempt++;

            if (!nextToken || allArtworks.length >= body.pageSize) break;
        }
        
        let __artworks = await Promise.all(allArtworks.map(async(a) => await ToArtworkViewModel(a, tableName)));

        if(body.category == 'SUPERCHAT' && !member.role?.includes('ADMIN')) {

            console.log("__artworks", __artworks);
            
            const now = new Date().toISOString();
            console.log("now", now);
            
            __artworks = __artworks.filter(obj => obj.Expiry && obj.Expiry > now);   // get non-expired superchat artworks
            console.log("__artworks 2", __artworks);

            if(body.sortBy) {
                if(!body.sortDirection) {
                    return {
                        Success: false,
                        Message: 'sortDirection is required when sortBy is present'
                    }
                }
            }

            if(body.sortBy === 'AMOUNT') {
                if(body.sortDirection == 'ASC' ) {
                    __artworks.sort((a, b) => {
                        if(a.Amount && b.Amount) {
                            return parseFloat(a.Amount) - parseFloat(b.Amount); // Sort by Amount in ascending order
                        } else if(a.Amount) {
                            return -1; // a has Amount, b does not
                        } else if(b.Amount) {
                            return 1; // b has Amount, a does not
                        }
                        return 0; // Both have no Amount, maintain original order
                    });
                }
                else if(body.sortDirection == 'DESC') {
                    __artworks.sort((a, b) => {
                        if(a.Amount && b.Amount) {
                            return parseFloat(b.Amount) - parseFloat(a.Amount); // Sort by Amount in descending order
                        } else if(a.Amount) {
                            return -1; // a has Amount, b does not
                        } else if(b.Amount) {
                            return 1; // b has Amount, a does not
                        }
                        return 0; // Both have no Amount, maintain original order
                    });
                }
            }
            else if(body.sortBy === 'CREATED_DATE') {
                if(body.sortDirection == 'ASC' ) {
                    __artworks.sort((a, b) => {
                        if(a.CreatedDate && b.CreatedDate) {
                            return new Date(a.CreatedDate).getTime() - new Date(b.CreatedDate).getTime(); // Sort by CreatedDate in ascending order
                        } else if(a.CreatedDate) {
                            return -1; // a has CreatedDate, b does not
                        } else if(b.CreatedDate) {
                            return 1; // b has CreatedDate, a does not
                        }
                        return 0; // Both have no CreatedDate, maintain original order
                    });
                }
                else if(body.sortDirection == 'DESC') {
                    __artworks.sort((a, b) => {
                        if(a.CreatedDate && b.CreatedDate) {
                            return new Date(b.CreatedDate).getTime() - new Date(a.CreatedDate).getTime(); // Sort by CreatedDate in descending order
                        } else if(a.CreatedDate) {
                            return -1; // a has CreatedDate, b does not
                        } else if(b.CreatedDate) {
                            return 1; // b has CreatedDate, a does not
                        }
                        return 0; // Both have no CreatedDate, maintain original order
                    });
                }
            }

            console.log("__artworks 3", __artworks);
        }

        const response = {
            Success: true,
            Data: { 
                    artworks: __artworks,
                    lastKey: artworkResult.LastEvaluatedKey,
                    nextToken: artworkResult.NextToken
                }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-artwork-listing-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-artwork-listing-post - ' + random10DigitNumber,
            Message: `Error in ch-artwork-listing-post ${e.message}\n\nStack trace:\n${e.stack}`,
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