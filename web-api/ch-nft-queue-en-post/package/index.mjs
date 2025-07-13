import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';
import ULID from 'ulid';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

let tableName;
let configs;

export const handler = async (event) => {
    console.log("nft enqueue mint event", event);
    
    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);

        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);
        let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
        configs = configResult.Items.map(unmarshall);
        console.log("configs", configs);

        var token = headers['authorization'];
        console.log("token", token);
        
        let memberId = null;
        let member;

        if(body.appPubKey == undefined && token) {
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

            if(member.role !== 'ADMIN') {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }


            // replace member with member who we want to sent the NFT to
            if(body.memberId == undefined) {
                return {
                    Success: false,
                    Message: "memberId is required"
                };
            }            
            sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + body.memberId);
                const response = {
                    Success: false,
                    Message: "member not found: " + body.memberId
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];
        
        }
        else if(body.appPubKey) {

            var token = headers['authorization'];
            console.log("token", token);

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
        else {
            return {
                Success: false,
                Code: 1,
                Message: "Missing login info."
            };
        }

        if(body.queueType == 'MINT_QUEUE' && !body.nftType) {
            return {
                Success: false,
                Message: 'nftType is required'
            };
        }

        if(body.nftType == 'CAR' || body.nftType == 'CHARACTER') {    
            if(!body.artworkId && body.queueType != 'UPDATE_QUEUE') {
                console.log('artworkId is required');
                return {
                            Success: false,
                            Message: 'artworkId is required'
                        };
            }
        }

        if(!body.queueType) {
            return {
                Success: false,
                Message: "queueType is required"
            }
        }

        if(body.queueType != 'MINT_QUEUE' && body.queueType != 'UPGRADE_QUEUE' && body.queueType != 'UPDATE_QUEUE') {
            return {
                Success: false,
                Message: "Invalid queueType"
            }
        }

        if(body.queueType == 'UPGRADE_QUEUE' && !body.unit) {
            return {
                Success: false,
                Message: "unit is required for upgrade membership NFT"
            }
        }

        if(body.queueType == 'UPDATE_QUEUE' && !body.unit) {
            return {
                Success: false,
                Message: "unit is required for update NFT metadata"  // unit is required for update NFT metadata
            }
        }

        // if(!member.discord_user_id) {
        //     console.log('User missing discord id. ' + member.user_id);
        //     return {
        //         Success: false,
        //         Message: 'ユーザーに discord ID がありません。 ' + member.user_id   //User missing discord id.
        //     }
        // }

        if(body.queueType == 'MINT_QUEUE' && body.nftType == 'CAR') {
            
            let membershipNFTsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${member.SK}' and type = 'ASSET' and asset_name = '${member.nft_member_b_asset_name}' and policy_id = '${member.nft_member_b_policy_id}' and status = 'NOTFORSALE'`}));
            console.log("membershipNFTsResult", JSON.stringify(membershipNFTsResult));
            if(membershipNFTsResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'Membership NFT not found.'
                };
            }
        }
        
        // grant NFT202412 discord role
        if(body.nftType === 'NFT202412' ) {
            if(member.role != 'ADMIN' && member.discord_user_id && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('NFT202412'))) {
                try {
                    const GUILD_ID = configs.find(x => x.key == 'DISCORD_GUILD_ID').value;
                    const BOT_TOKEN = configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value;
                    const DISCORD_ROLE_ID_RACINGFAN = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_RACINGFAN : process.env.DISCORD_ROLE_ID_RACINGFAN_TEST);
                    
                    let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${DISCORD_ROLE_ID_RACINGFAN}`
                    console.log('grant discord role for proj url', url);
                    let _headers = {
                                        "Authorization": `Bot ${BOT_TOKEN}`,
                                        "Content-Type": "application/json"
                                    };
                    let grantRoleResult = await axios.put(url,
                                                        null,
                                                        {
                                                            headers: _headers,
                                                        });
                    console.log("grant discord role for NFT202412 result", grantRoleResult);
                    
                    let sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , `;
                    sql += ` discord_roles = '${member.discord_roles ? member.discord_roles + ',NFT202412' : 'NFT202412'}' `;
                    sql += ` where PK = '${member.PK}' and SK = '${member.SK}'`;
                    console.log("sql", sql);
                    let updateDiscordRoleResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log("updateDiscordRoleResult", updateDiscordRoleResult);
                } catch (err) {
                    console.log(err);
                    const _message = {
                        Subject: 'Honda Error - comm-nft-queue-en-post',
                        Message: "unable to grant discord role for discord user id " + member.discord_user_id + ' for nftType ' + body.nftType,
                        TopicArn: process.env.SNS_TOPIC_ERROR
                    };
                    await sns.publish(_message).promise();
                }
            }
        }

        let sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.SK}' and type = 'QUEUE' and queue_type = '${body.queueType}' and user_id = '${member.user_id}' and nft_type = '${body.nftType}' order by PK desc`;
        console.log("sql", sql);
        let queueResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(queueResult.Items.length > 0) {
            if(body.queueType == 'MINT_QUEUE') {
                if(body.nftType == 'CAR') {
                    // for CAR NFT requests
                    let successQueueItems = queueResult.Items.map(unmarshall).filter(x => x.status == 'SUCCESS');

                    let membershipNFTsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${member.SK}' and type = 'ASSET' and asset_name = '${member.nft_member_b_asset_name}' and policy_id = '${member.nft_member_b_policy_id}' and status = 'NOTFORSALE'`}));
                    console.log("membershipNFTsResult", JSON.stringify(membershipNFTsResult));
                    
                    let BStatus;
                    let membershipNFTs = membershipNFTsResult.Items.map(unmarshall);
                    for (let i = 0; i < membershipNFTs.length; i++) {
                        const memberNFT = membershipNFTs[i];
                        console.log("memberNFT", memberNFT);
                        
                        if(memberNFT.store_id == 'HONDA_MEMBERSHIP_B') {
                            if(memberNFT.is_gold === true) {
                                BStatus = 'GOLD'
                            }
                            else if(memberNFT.is_silver === true) {
                                BStatus = 'SILVER'
                            }
                            else {
                                BStatus = 'BRONZE'
                            }
                        }
                    }
                    
                    console.log("BStatus", BStatus);
                    
                    let maxNFT = 4;
                    if(BStatus === 'GOLD')
                        maxNFT = 8;
                    else if(BStatus === 'SILVER')
                        maxNFT = 6;
                    else if(BStatus === 'BRONZE')
                        maxNFT = 4;
                    else {
                        console.log("Invalid membership ranking");
                        
                        return {
                            Success: false,
                            Message: 'MetaGarage のメンバーシップ ランキングが無効です'    //Invalid membership ranking for MetaGarage
                        };
                    }

                    if(member.SK == 'MEMBERWALLET#0x9b4380e74eCecf9B8c84393809A55c85D238fD3C') {
                        maxNFT = 9;
                    }
                    else if(member.SK == 'MEMBERWALLET#0x59D5993B49a44dAc3cd9911284E21746C97B55C0') {
                        maxNFT = 16;
                    }

                    if(successQueueItems.length + 1 > maxNFT) {
                        console.log("Maximum number of NFTs exceeded for your membership ranking. メンバーシップランキングのNFTの最大数を超えました。");
                        return {
                            Success: false,
                            Message: "メンバーシップランキングのNFTの最大数を超えました"
                        }
                    }
                }
                else if(body.nftType == 'CHARACTER') { 
                    
                    let memberWhitelistResult = await await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST'`}));
                    console.log("memberWhitelistResult", memberWhitelistResult);
                    let memberWhiteLists = [];
                    if(memberWhitelistResult.Items.length > 0) {
                        memberWhiteLists = memberWhitelistResult.Items.map(unmarshall);
                    }

                    if(memberWhiteLists.find(x => x.whitelist_type.includes('PALEBLUEDOT_ADDITIONAL_NFT'))) {

                        // member in PALEBLUEDOT_ADDITIONAL_NFT dont have to check status

                        // let queueItems = queueResult.Items.map(unmarshall);

                        // let existedQueueItem = queueItems.find(x => x.token_id == body.unit);    // mint - dont have body.unit

                        // if(existedQueueItem) {
                        //     if(existedQueueItem.status == 'NEW' || existedQueueItem.status == 'IN_PROGRESS') {
                        //         return {
                        //             Success: false,
                        //             Message: "同じNFTを2回鋳造することは出来ません。現在、鋳造中です。鋳造には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                        //         }
                        //     }
                        //     else if(existedQueueItem.status == 'SUCCESS') {
                        //         return {
                        //             Success: false,
                        //             Message: "あなたのNFTはすでに正常に鋳造されています。重複したNFTは許可されません。"
                        //         }
                        //     }
                        //     else if(existedQueueItem.status == 'FAILED') {
                        //         return {
                        //             Success: false,
                        //             Message: "NFT の鋳造に失敗しました。詳細については、キュー メッセージを参照してください。"
                        //         }
                        //     }
                        // }
                    }
                    else {
                        let queueItem = queueResult.Items.map(unmarshall)[0];
                        if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                            return {
                                Success: false,
                                Message: "同じNFTを2回鋳造することは出来ません。現在、鋳造中です。鋳造には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                            }
                        }
                        else if(queueItem.status == 'SUCCESS') {
                            if(body.nftType != 'CAR') {
                                return {
                                    Success: false,
                                    Message: "あなたのNFTはすでに正常に鋳造されています。重複したNFTは許可されません。"
                                }
                            }
                        }
                        else if(queueItem.status == 'FAILED') {
                            return {
                                Success: false,
                                Message: "NFT の鋳造に失敗しました。詳細については、キュー メッセージを参照してください。"
                            }
                        }
                    }
                }
                else { 
                    let queueItem = queueResult.Items.map(unmarshall)[0];
                    if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                        return {
                            Success: false,
                            Message: "同じNFTを2回鋳造することは出来ません。現在、鋳造中です。鋳造には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                        }
                    }
                    else if(queueItem.status == 'SUCCESS') {
                        if(body.nftType != 'CAR') {
                            return {
                                Success: false,
                                Message: "あなたのNFTはすでに正常に鋳造されています。重複したNFTは許可されません。"
                            }
                        }
                    }
                    else if(queueItem.status == 'FAILED') {
                        return {
                            Success: false,
                            Message: "NFT の鋳造に失敗しました。詳細については、キュー メッセージを参照してください。"
                        }
                    }
                }
            }

            if(body.queueType == 'UPGRADE_QUEUE' && member.is_gold === true) {
                return {
                    Success: false,
                    Message: "会員はゴールド会員以上にアップグレードすることはできません" //"Member cannot upgrade more than Gold Membership"
                }
            }

            if(body.queueType == 'UPDATE_QUEUE') {
                if(body.appPubKey && body.nftType == 'CHARACTER') {

                    let assetsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${member.SK}' and PK = 'ASSET#${body.unit}' and type = 'ASSET'`}));
                    console.log("assetsResult", JSON.stringify(assetsResult));
                    if(assetsResult.Items.length === 0) {
                        console.log('NFT not found. unit : ' + body.unit);
                        
                        return {
                            Success: false,
                            Message: 'NFT not found. unit : ' + body.unit
                        };
                    }

                    let asset = assetsResult.Items.map(unmarshall)[0];

                    let memberWhitelistResult = await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST'`}));
                    console.log("memberWhitelistResult", memberWhitelistResult);
                    let memberWhiteLists = [];
                    if(memberWhitelistResult.Items.length > 0) {
                        memberWhiteLists = memberWhitelistResult.Items.map(unmarshall);
                    }

                    let maxRevealCount = 1;
                    if(memberWhiteLists.find(x => x.whitelist_type.includes('PALEBLUEDOT_WINNER'))) {
                        maxRevealCount = 2;
                    }

                    if(asset.reveal_count === undefined || asset.reveal_count < maxRevealCount) {

                        if(memberWhiteLists.find(x => x.whitelist_type.includes('PALEBLUEDOT_ADDITIONAL_NFT'))) {
                            let queueItems = queueResult.Items.map(unmarshall);
                            let existedQueueItems = queueItems.filter(x => x.unit == body.unit);
                            
                            if(existedQueueItems) {
                                
                                let queueItem = existedQueueItems[0];

                                if(maxRevealCount === 2) {
                                    if(existedQueueItems.length === 2) {
                                        if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                                            return {
                                                Success: false,
                                                Message: "同じNFTを2回公開することは出来ません。現在、公開中です。公開には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                                            }
                                        }
                                        else if(queueItem.status == 'SUCCESS') {
                                            return {
                                                Success: false,
                                                Message: "あなたのNFTはすでに正常に公開されています。重複したNFTは許可されません。"
                                            }
                                        }
                                        else if(queueItem.status == 'FAILED') {
                                            return {
                                                Success: false,
                                                Message: "NFT の公開に失敗しました。詳細については、キュー メッセージを参照してください。"
                                            }
                                        }
                                    }
                                }
                                else {
                                    if(queueItem) {
                                        if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                                            return {
                                                Success: false,
                                                Message: "同じNFTを2回公開することは出来ません。現在、公開中です。公開には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                                            }
                                        }
                                        else if(queueItem.status == 'SUCCESS') {
                                            return {
                                                Success: false,
                                                Message: "あなたのNFTはすでに正常に公開されています。重複したNFTは許可されません。"
                                            }
                                        }
                                        else if(queueItem.status == 'FAILED') {
                                            return {
                                                Success: false,
                                                Message: "NFT の公開に失敗しました。詳細については、キュー メッセージを参照してください。"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else {

                            let queueItem = queueResult.Items.map(unmarshall)[0];

                            if(maxRevealCount === 2) {
                                if(queueResult.Items.length === 2) {
                                    if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                                        return {
                                            Success: false,
                                            Message: "同じNFTを2回公開することは出来ません。現在、公開中です。公開には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                                        }
                                    }
                                    else if(queueItem.status == 'SUCCESS') {
                                        return {
                                            Success: false,
                                            Message: "あなたのNFTはすでに正常に公開されています。重複したNFTは許可されません。"
                                        }
                                    }
                                    else if(queueItem.status == 'FAILED') {
                                        return {
                                            Success: false,
                                            Message: "NFT の公開に失敗しました。詳細については、キュー メッセージを参照してください。"
                                        }
                                    }
                                }
                            }
                            else {
                                if(queueItem.status == 'NEW' || queueItem.status == 'IN_PROGRESS') {
                                    return {
                                        Success: false,
                                        Message: "同じNFTを2回公開することは出来ません。現在、公開中です。公開には、数時間以上かかることがあります。数分経っても発行されない場合はブラウザを閉じてお待ちください。"
                                    }
                                }
                                else if(queueItem.status == 'SUCCESS') {
                                    return {
                                        Success: false,
                                        Message: "あなたのNFTはすでに正常に公開されています。重複したNFTは許可されません。"
                                    }
                                }
                                else if(queueItem.status == 'FAILED') {
                                    return {
                                        Success: false,
                                        Message: "NFT の公開に失敗しました。詳細については、キュー メッセージを参照してください。"
                                    }
                                }
                            }
                        }
                    }
                    else {
                        return {
                            Success: false,
                            Message: `NFT を公開できるのは ${maxRevealCount} 回だけです` //You only can reveal the NFT once
                        }
                    }
                }
            }
        }

        let queueId = ULID.ulid();
        let currentDate = new Date().toISOString();
        sql = `INSERT INTO "${tableName}"
                VALUE { 
                        'PK': 'QUEUE#${body.queueType == 'MINT_QUEUE' ? 'MINT' : (body.queueType == 'UPGRADE_QUEUE' ? 'UPGRADE' : 'UPDATE')}#${queueId}', 
                        'SK': '${member.SK}', 
                        'type': 'QUEUE', 
                        'queue_type': '${body.queueType}',
                        'queue_id': '${queueId}',
                        'user_id': '${member.user_id}',
                        'wallet_address': '${member.wallet_address}',
                        'discord_user_id': '${member.discord_user_id ? member.discord_user_id : ''}',
                        'app_pub_key': '${body.appPubKey ? body.appPubKey : ''}',
                        'token': '${token}',
                        'nft_type': '${body.nftType ? body.nftType : ''}',`;
        
        //for upgrade membership nft
        if(body.unit) {
            sql += `'unit': '${body.unit}',`;
        }

        //for update nft metadata
        if(body.metadata) {
            sql += `'metadata': '${body.metadata}',`;
        }

        if(body.artworkId) {
            sql += `'artwork_id': '${body.artworkId}',`;
        }

        if(body.artworkId2) {
            sql += `'artwork_id_2': '${body.artworkId2}',`;
        }

        if(body.artworkId3) {
            sql += `'artwork_id_3': '${body.artworkId3}',`;
        }

        sql += `'status': 'NEW', 'created_date': '${currentDate}'}`;

        console.log("sql", sql);

        let enqueueResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        console.log("enqueueResult", enqueueResult);

        return {
            Success: true,
            Data: {
                queueId: queueId
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-queue-en-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-queue-en-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-queue-en-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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