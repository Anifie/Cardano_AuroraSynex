import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import axios from 'axios';
import jwt from 'jsonwebtoken'

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 10 * 60 * 1000 // 10 minutes in milliseconds
    }
});

const sleep = (ms) => {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
}

async function fetchAllRecords(sql) {
    let results = [];
    let nextToken;

    do {
        const command = new ExecuteStatementCommand({
            Statement: sql,
            NextToken: nextToken, // Add NextToken if available
        });

        const response = await dbClient.send(command);

        // Accumulate items from this page
        if (response.Items) {
            results = results.concat(response.Items);
        }

        // Update nextToken for the next iteration
        nextToken = response.NextToken;
    } while (nextToken); // Continue until there's no nextToken

    return results;
}

async function sendUpgradeQueue(member, tokenId, metadata, newRank, isTest) {

    console.log("sendUpgradeQueue", member, tokenId, metadata, newRank, isTest);

    let newArtworkId;

    switch (newRank) {
        case 'Green':
            newArtworkId = process.env.GREEN_ARTWORK_ID;
            metadata.attributes.find(attr => attr.trait_type === "Rank").value = "Green";
            break;
        case 'Gold':
            newArtworkId = process.env.GOLD_ARTWORK_ID;
            metadata.attributes.find(attr => attr.trait_type === "Rank").value = "Gold";
            break;
        case 'Platinum':
            newArtworkId = process.env.PLATINUM_ARTWORK_ID;
            metadata.attributes.find(attr => attr.trait_type === "Rank").value = "Platinum";
            break;
        case 'Black':
            newArtworkId = process.env.BLACK_ARTWORK_ID;
            metadata.attributes.find(attr => attr.trait_type === "Rank").value = "Black";
            break;
        default:
            throw new Error('Rank not found ' + newRank);
    }

    const token = jwt.sign({ MemberId: '01GJ5XT15FHWPFRN5QJSPXKW0X' }, configs.find(x=>x.key=='JWT_SECRET').value, { expiresIn: '1440m' });
    let response = await axios.post(process.env.COMMUNITY_API_URL + '/nft/queue',
                        JSON.stringify({
                            nftType: 'RACINGFAN',
                            tokenId: tokenId,
                            artworkId: newArtworkId,
                            metadata: typeof metadata == 'object' ? JSON.stringify(metadata) : metadata,
                            memberId: member.user_id,
                            queueType: 'UPDATE_QUEUE',
                        }),
                        {
                            headers: {
                                'Content-Type': 'application/json',
                                'Authorization': 'Bearer ' + token,
                                'origin': isTest ? 'test.com' : 'anifie.com'
                            }
                        }
                    );
    console.log('sendUpgradeQueue jsonResult', response.data);
    return response.data;
}

export const handler = async (event) => {
    console.log("point award event", event);
    
    let tableName;

    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);

        // console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);

        let sql;

        sql = `select * from "${tableName}" where PK = 'ENUM' and SK = 'POINT_AWARD_BATCH_STATUS'`;
        let pointBatchStatusResult = await fetchAllRecords(sql);
        console.log("pointBatchStatusResult", pointBatchStatusResult);
        let pointBatchStatus = pointBatchStatusResult.map(unmarshall)[0].enum_values;

        if(pointBatchStatus == 'IN_PROGRESS') {
            return {
                Success: false,
                Message: "Point award batch is already in progress."
            };
        }

        sql = `UPDATE "${tableName}" set enum_values = 'IN_PROGRESS' WHERE PK = 'ENUM' AND SK = 'POINT_AWARD_BATCH_STATUS'`;
        let updateBatchStatusResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchStatusResult", updateBatchStatusResult);

        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'POINT_SETTINGS'`;
        let pointSettingsResult = await fetchAllRecords(sql);
        console.log("pointSettingsResult", pointSettingsResult);
        let pointSettings = pointSettingsResult.map(unmarshall);
        console.log("pointSettings", pointSettings);
        
        // only get racing fan members
        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'MEMBER' and discord_user_id is not missing and CONTAINS("discord_roles", 'RACINGFAN')`;
        if(body.memberId) {
            sql = `select * from "${tableName}" where type = 'MEMBER' and PK = 'MEMBER#${body.memberId}'`;
        }
        let membersResult = await fetchAllRecords(sql);
        let members = membersResult.map(unmarshall);
        console.log("members.length", members.length);
        

        let generalPointSetting = pointSettings.find(x => x.point_setting_type == 'GENERAL');
        let datePointSettings = pointSettings.filter(x => x.point_setting_type == 'DATE').sort((x, y) => new Date(x.start_date) - new Date(y.start_date));  // sort ascending

        // console.log("generalPointSetting", generalPointSetting);
        // console.log("datePointSettings", datePointSettings);
        
        // let txStatements = [];
        let membersPoints = [];

        for (let i = 0; i < members.length; i++) {

            const member = members[i];
            // if(member.user_id != 'c24c4284c1873b2ee9b1028118e02fa5')
            //     continue;

            console.log("member", member);

            let totalPoints = 0;
            let messagesCount = 0;
            let reactionsCount = 0;
            let attachmentsCount = 0;
            let votesCount = 0;
            let repliesCount = 0;

            for (let k = 0; k < datePointSettings.length; k++) {
                const datePointSetting = datePointSettings[k];
                
                // console.log("datePointSetting", datePointSetting);

                let omittedChannelIs = [];
                if(datePointSetting.omitted_channel_ids) {
                    omittedChannelIs = datePointSetting.omitted_channel_ids.split(',');
                }

                console.log("omittedChannelIs", omittedChannelIs);
                
                
                // messages
                sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_MESSAGE' and discord_user_id = '${member.discord_user_id}'`;
                sql += ` and created_date between '${datePointSetting.start_date}' and '${datePointSetting.end_date}'`;
                console.log("sql", sql);
                let messagesResult = await fetchAllRecords(sql);
                let messages = messagesResult.map(unmarshall);
                if(omittedChannelIs.length > 0) {
                    // Filter out messages belonging to omitted channel IDs
                    messages = messages.filter(msg => !omittedChannelIs.includes(msg.discord_channel_id));
                }
                if(datePointSetting.message_minimum_length != undefined) {
                    messages = messages.filter(msg => msg.content && msg.content.length > datePointSetting.message_minimum_length);
                }
                console.log("message.length", messages.length);
                
                // reactions
                sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_REACTION' and discord_user_id = '${member.discord_user_id}'`;
                sql += ` and created_date between '${datePointSetting.start_date}' and '${datePointSetting.end_date}'`;
                console.log("sql", sql);
                let reactionsResult = await fetchAllRecords(sql);
                let reactions = reactionsResult.map(unmarshall);
                if(omittedChannelIs.length > 0) {
                    // Filter out reactions belonging to omitted channel IDs
                    reactions = reactions.filter(reaction => !omittedChannelIs.includes(reaction.discord_channel_id));
                }
                console.log("reactions.length", reactions.length);

                // replies
                sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'DISCORD_MESSAGE' and discord_user_id = '${member.discord_user_id}'`;
                sql += ` and created_date between '${datePointSetting.start_date}' and '${datePointSetting.end_date}' and raw_reference is not missing`;
                console.log("sql", sql);
                let repliesResult = await fetchAllRecords(sql);
                let replies = repliesResult.map(unmarshall);
                if(omittedChannelIs.length > 0) {
                    // Filter out reactions belonging to omitted channel IDs
                    replies = replies.filter(r => !omittedChannelIs.includes(r.discord_channel_id));
                }
                console.log("replies.length", replies.length);
                
                // votes
                sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'VOTE_DISCORD_ANSWER' and SK = '${member.discord_user_id}'`;
                sql += ` and created_date between '${datePointSetting.start_date}' and '${datePointSetting.end_date}'`;
                console.log("sql", sql);
                let votesResult = await fetchAllRecords(sql);
                let votes = votesResult.map(unmarshall);
                console.log("votes.length", votes.length);

                // attachments
                let messages_attachment = messagesResult.map(unmarshall);
                // console.log("messages_attachment 1", messages_attachment);
                messages_attachment = messages_attachment.filter(m => m.raw_attachments && m.raw_attachments.length > 0);
                // console.log("messages_attachment 2", messages_attachment);
                if(omittedChannelIs.length > 0) {
                    // Filter out reactions belonging to omitted channel IDs
                    messages_attachment = messages_attachment.filter(a => !omittedChannelIs.includes(a.discord_channel_id));
                }
                let attachments = [];
                if(messages_attachment.length > 0) {
                    attachments = messages_attachment.map(x => typeof x.raw_attachments == 'string' ? JSON.parse(x.raw_attachments) : x.raw_attachments).flat();
                    // console.log("attachments 3", attachments);
                }
                console.log("attachments.length", attachments.length);

                datePointSetting.weight_attachments = datePointSetting.weight_attachments ? datePointSetting.weight_attachments : 0;

                totalPoints += (datePointSetting.weight_message * messages.length) 
                                + (datePointSetting.weight_reaction * reactions.length) 
                                + (datePointSetting.weight_vote * votes.length)
                                + (datePointSetting.weight_attachments * attachments.length)
                                + (datePointSetting.weight_replies * replies.length);

                messagesCount += messages.length;
                reactionsCount += reactions.length;
                votesCount += votes.length;
                attachmentsCount += attachments.length;
                repliesCount += replies.length;

                // console.log("totalPoints", totalPoints);
                // console.log("messagesCount", messagesCount);
                // console.log("reactionsCount", reactionsCount);
                // console.log("votesCount", votesCount);
                // console.log("attachmentsCount", attachmentsCount);
                // console.log("repliesCount", repliesCount);
            }

            // roles
            sql = `select * from "${tableName}" where SK = 'DISCORD_USER' and PK = 'DISCORD_USER#${member.discord_user_id}'`;
            let userResult = await fetchAllRecords(sql);
            let user;
            if(userResult.length > 0) {
                user = userResult.map(unmarshall)[0];
                if(user.roles){
                    user.roles = user.roles.split(',');
                }
                console.log("Discord user", user);
            }
            else {
                console.log("Discord user not found " + member.discord_user_id);
                continue;
            }
            
            if(generalPointSetting.weight_role && typeof generalPointSetting.weight_role == 'string') {
                generalPointSetting.weight_role = JSON.parse(generalPointSetting.weight_role);
            }

            if(user && user.roles && generalPointSetting.weight_role && generalPointSetting.weight_role.length > 0) {
                for (const weightRole of generalPointSetting.weight_role) {
                    // Check if the user's roles include the current role
                    if (user.roles.includes(weightRole.role)) {
                      // Add the weight to totalPoints
                      totalPoints += weightRole.weight;
                    }
                }
            }

            //nft
            sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'ASSET' and SK = 'MEMBERWALLET#${member.wallet_address}' and status = 'NOTFORSALE'`;
            let nftResult = await fetchAllRecords(sql);
            console.log("nftResult.length", nftResult.length);
            //transferred nft
            sql = `SELECT * FROM "${tableName}"."BySentFromAddress" where sent_from_address = '${member.wallet_address}' and type = 'ASSET'`;
            let transferredNFTResult = await fetchAllRecords(sql);
            console.log("transferredNFTResult.length", transferredNFTResult.length);
            totalPoints += ((nftResult.length + transferredNFTResult.length) * generalPointSetting.weight_nft);

            console.log("totalPoints", totalPoints);
            

            sql = `UPDATE "${tableName}" 
                    SET modified_date = '${new Date().toISOString()}' 
                    , total_points = ${totalPoints}
                    WHERE PK = '${member.PK}' AND SK = '${member.SK}'`;
            console.log("sql", sql);
            

            let updateResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("updateResult", updateResult);

            // txStatements.push({ "Statement": sql});

            // const statements = { "TransactStatements": txStatements };  
            // console.log("statements", JSON.stringify(statements));
            // const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
            // console.log("Transaction result", dbTxResult);

            membersPoints.push({ 
                                    member: member, 
                                    total_points: totalPoints, 
                                    messages_count: messagesCount, 
                                    reactions_count: reactionsCount, 
                                    attachments_count: attachmentsCount,
                                    nft_count: nftResult.length + transferredNFTResult.length,
                                    replies_count: repliesCount,
                                    votes_count: votesCount,
                                    roles: user?.roles,
                                    discord_user_name: user.username,
                                    discord_user_avatar: user.avatar,
                                    discord_user_discriminator: user.discriminator,
                                    discord_joined_date: user.joined_date
                                });
        }

        // for (let i = 0; i < membersPoints.length; i++) {
        //     const membersPoint = membersPoints[i];
        
        //     let level = 1;
        //     let levelMaxPoints;

        //     //console.log("membersPoint", membersPoint);
            
        //     while (true) {
        //         // Calculate max points for the current level
        //         levelMaxPoints = pointSettings.level_var_1 * Math.pow(level, 2) + (pointSettings.level_var_2 * level);
        //         //console.log("levelMaxPoints", levelMaxPoints);
        
        //         // Check if the total points fit within this level
        //         if (membersPoint.total_points <= levelMaxPoints) {
        //             membersPoint.level = level;
        //             membersPoint.pointsRequiredToNextLevel = levelMaxPoints - membersPoint.total_points;
        //             break;
        //         }
        
        //         // Move to the next level
        //         level++;
        
        //         // Safety check to prevent infinite loops
        //         if (level > 1000) {
        //             membersPoint.level = 1000;
        //             membersPoint.pointsRequiredToNextLevel = 0;
        //             break;
        //         }
        //     }
        // }
        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'LEVEL_RANK'`;
        let levelRanksRankResult = await fetchAllRecords(sql);
        console.log("levelRanksRankResult", levelRanksRankResult);
        let levelRanks = levelRanksRankResult.map(unmarshall);
        levelRanks.sort((a, b) => a.point - b.point);
        for (let i = 0; i < membersPoints.length; i++) {
            const membersPoint = membersPoints[i];
            let _level = levelRanks[0]; // Default to the first level
            for (let j = 0; j < levelRanks.length; j++) {
                if (membersPoint.total_points > levelRanks[j].point) {
                    
                } else if (membersPoint.total_points == levelRanks[j].point) {
                    _level = levelRanks[j];
                    break;
                } else {
                    // membersPoint.total_points < levelRanks[j].point
                    _level = levelRanks[j];
                    break; // Stop checking once we go past the member's points
                }
            }
            membersPoint.level = _level.level;
            membersPoint.rank_2 = _level.rank;
            membersPoint.pointsRequiredToNextLevel = _level.point - membersPoint.total_points;

            // upgrade membership nft based on rank
            if(membersPoint.member.nft_racing_fan_token_id && membersPoint.member.nft_racing_fan_contract_address) {
                sql = `select * from "${tableName}" where type = 'ASSET' and PK = 'ASSET#${membersPoint.member.nft_racing_fan_contract_address}#${membersPoint.member.nft_racing_fan_token_id}' and SK = '${membersPoint.member.SK}'`;
                let assetResult = await fetchAllRecords(sql);
                console.log("assetResult", assetResult);                
                if(assetResult.length > 0) {
                    let assets = assetResult.map(unmarshall)
                    let asset = assets[0]
                    let metadata = JSON.parse(asset.metadata);
                    console.log("metadata", metadata);
                    let rank = metadata.attributes.find(attr => attr.trait_type === "Rank")?.value;
                    console.log("rank", rank);
                    console.log("membersPoint.rank_2", membersPoint.rank_2);
                    if(rank && rank != membersPoint.rank_2) {
                        let upgradeQueueResult = await sendUpgradeQueue(membersPoint.member, 
                                                                        membersPoint.member.nft_racing_fan_token_id, 
                                                                        metadata,
                                                                        membersPoint.rank_2,
                                                                        tableName == process.env.TABLE_NAME_TEST);
                        console.log("upgradeQueueResult", upgradeQueueResult);
                    }
                }
            }
        }

        membersPoints.sort((a, b) => {
            // First sort by total_points in descending order   (i.e. highest point first)
            if (b.total_points !== a.total_points) {
                return b.total_points - a.total_points;
            }
            // If total_points are the same, sort by discord_joined_date in ascending order (i.e. lowest discord_joined_date, i.e earliest)
            const dateA = new Date(a.discord_joined_date);
            const dateB = new Date(b.discord_joined_date);
            return dateA - dateB; // Ascending order for dates
        });

        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'LEADERBOARD'`;
        if(body.memberId) {
            sql = `select * from "${tableName}"."InvertedIndex" where type = 'LEADERBOARD' and SK = '${membersPoints[0].member.discord_user_id}'`;
        }
        console.log("sql", sql);
        
        let leaderboardResult = await fetchAllRecords(sql);
        if(leaderboardResult.length > 0) {
            console.log("leaderboardResult.length", leaderboardResult.length);
            let leaderBoards = leaderboardResult.map(unmarshall);
            for (let i = 0; i < leaderBoards.length; i++) {
                const leaderBoard = leaderBoards[i];
                sql = `DELETE FROM "${tableName}" WHERE PK = '${leaderBoard.PK}' AND SK = '${leaderBoard.SK}'`;
                console.log("sql", sql);
                let deleteResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("deleteResult", deleteResult);
            }
        }

        console.log("membersPoints", membersPoints);

        for (let i = 0; i < membersPoints.length; i++) {
            const membersPoint = membersPoints[i];
            sql = `INSERT INTO "${tableName}" 
                    VALUE { 
                        'PK': 'LEADERBOARD#${i + 1}' ,
                        'SK': '${membersPoint.member.discord_user_id}', 
                        'type': 'LEADERBOARD', 
                        'discord_user_id': '${membersPoint.member.discord_user_id}', 
                        'discord_user_avatar': '${membersPoint.discord_user_avatar ? membersPoint.discord_user_avatar : ''}', 
                        'discord_user_discriminator': '${membersPoint.discord_user_discriminator != undefined ? membersPoint.discord_user_discriminator : ''}',
                        'discord_user_name': '${membersPoint.discord_user_name ? membersPoint.discord_user_name : ''}', 
                        'user_id': '${membersPoint.member.user_id}', 
                        'wallet_address': '${membersPoint.member.wallet_address}', 
                        'level': ${membersPoint.level}, 
                        'total_points': ${membersPoint.total_points}, 
                        'messages_count': ${membersPoint.messages_count}, 
                        'reactions_count': ${membersPoint.reactions_count}, 
                        'attachments_count': ${membersPoint.attachments_count},
                        'nft_count': ${membersPoint.nft_count},
                        'replies_count': ${membersPoint.replies_count},
                        'votes_count': ${membersPoint.votes_count}, 
                        'roles': '${membersPoint.roles && membersPoint.roles.length > 0 ? JSON.stringify(membersPoint.roles) : ''}', 
                        'points_required_to_next_level': ${membersPoint.pointsRequiredToNextLevel},
                        'rank': ${i + 1},
                        'rank_2': '${membersPoint.rank_2}',
                        'created_date': '${new Date().toISOString()}'
                    }`;

            console.log("sql", sql);
            let insertResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("insertResult", insertResult);

            if(membersPoint.member.discord_roles.includes("RACINGFAN")) {   // only for members who have Racing_fan role

                // grant discord role if not yet granted
                let roleId;
                let roleName;

                try {

                    if(membersPoint.rank_2 == 'Gold' && !membersPoint.member.discord_roles.includes('RF_GOLD')) {
                        roleName = 'RF_GOLD';
                        roleId = tableName == process.env.TABLE_NAME ? process.env.ROLE_ID_RF_GOLD : process.env.ROLE_ID_RF_GOLD_TEST;
                    }
                    else if(membersPoint.rank_2 == 'Platinum' && !membersPoint.member.discord_roles.includes('RF_PLATINUM')) {
                        roleName = 'RF_PLATINUM';
                        roleId = tableName == process.env.TABLE_NAME ? process.env.ROLE_ID_RF_PLATINUM : process.env.ROLE_ID_RF_PLATINUM_TEST;
                    }
                    else if(membersPoint.rank_2 == 'Black' && !membersPoint.member.discord_roles.includes('RF_BLACK')) {
                        roleName = 'RF_BLACK';
                        roleId = tableName == process.env.TABLE_NAME ? process.env.ROLE_ID_RF_BLACK : process.env.ROLE_ID_RF_BLACK_TEST;
                    }

                    console.log('roleId', roleId);
                    console.log('roleName', roleName);
                    
                    if(roleId) {
                        let guilId = tableName == process.env.TABLE_NAME ? process.env.GUILD_ID_PROD : process.env.GUILD_ID_TEST;
                        let botTokenId = tableName == process.env.TABLE_NAME ? process.env.BOT_TOKEN_PROD : process.env.BOT_TOKEN_TEST;

                        // grant discord role
                        let url = `https://discord.com/api/v8/guilds/${guilId}/members/${membersPoint.member.discord_user_id}/roles/${roleId}`
                        console.log('grant discord role url', url);
                        let _headers = {
                                            "Authorization": `Bot ${botTokenId}`,
                                            "Content-Type": "application/json"
                                        }

                        let grantRoleResult = await axios.put(url,
                                                            null,
                                                            {
                                                                headers: _headers,
                                                            });

                        console.log("grant discord role result", grantRoleResult);
                        await sleep(2000);

                        if(grantRoleResult.status != 204 && grantRoleResult.status != 200) {
                            console.log("grantRoleResult", grantRoleResult);
                            const _message = {
                                Subject: 'Honda Cardano Error - ch-point-award-post',
                                Message: "unable to grant discord role for discord user id " + membersPoint.member.discord_user_id + ' for roleId ' + roleId + ' roleName ' + roleName,
                                TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                            };
                            await snsClient.send(new PublishCommand(_message));
                        }
                        else  {
                            //update member record
                            sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , discord_roles = '${membersPoint.member.discord_roles ? membersPoint.member.discord_roles + ',' + roleName : roleName}' where PK = '${membersPoint.member.PK}' and SK = '${membersPoint.member.SK}'`;
                            console.log("sql", sql);
                            let updateMemberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                            console.log("updateMemberResult", updateMemberResult);
                        }
                    }

                } catch (_err) {
                    console.log(_err);
                    const _message = {
                        Subject: 'Honda Cardano Error - ch-point-award-post',
                        Message: "unable to grant discord role for discord user id " + membersPoint.member.discord_user_id + ' for roleId ' + roleId + ' roleName ' + roleName,
                        TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                    };
                    await snsClient.send(new PublishCommand(_message));
                }
            }
            
        }
        
        sql = `UPDATE "${tableName}" set enum_values = 'DONE' WHERE PK = 'ENUM' AND SK = 'POINT_AWARD_BATCH_STATUS'`;
        let updateBatchStatusDoneResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateBatchStatusDoneResult", updateBatchStatusDoneResult);

        return {
            Success: true
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-point-award-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-point-award-post - ' + random10DigitNumber,
            Message: `Error in ch-point-award-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };
        
        // if(tableName == process.env.TABLE_NAME)
        //     await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: e.message
            //Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};