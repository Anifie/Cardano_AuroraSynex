import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import nacl from 'tweetnacl';
import ULID from 'ulid';
import axios from 'axios';
import { TextEncoder } from 'util';
// import { createRequire } from "module";

// const require = createRequire(import.meta.url);

// import chromium from '@sparticuz/chromium';
// import puppeteer from 'puppeteer-core';
// import FormData from 'form-data';

// import fs from 'fs';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient();

let tableName;
let configs;



// function getDiscordAvatarUrl(userId, avatarHash, discriminator) {
//   if (avatarHash) {
//       // Check if avatar is animated
//       const extension = avatarHash.startsWith("a_") ? "gif" : "png";
//       return `https://cdn.discordapp.com/avatars/${userId}/${avatarHash}.${extension}`;
//   } else {
//       // Fallback to default avatar
//       const defaultAvatar = discriminator % 5;
//       return `https://cdn.discordapp.com/embed/avatars/${defaultAvatar}.png`;
//   }
// }

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

// async function generateRankCard(username, avatarURL, rank, rank2, level, current_total_points, target) {
//   let result = null;
//   let browser = null;

//   try {

//     // Launch Puppeteer
//     browser = await puppeteer.launch({
//                                         args: chromium.args,
//                                         defaultViewport: chromium.defaultViewport,
//                                         executablePath: await chromium.executablePath(),
//                                         headless: chromium.headless,
//                                       });

//     let page = await browser.newPage();
//     // await page.goto(event.url || 'https://example.com');
//     await page.setViewport({ width: 800, height: 200 });

//     let htmlFileUrl = 'https://s3.ap-northeast-1.amazonaws.com/anifie.community.resource/rank_card.html';

//     // Fetch HTML content from the external file URL
//     const response = await axios.get(htmlFileUrl);
//     let htmlContent = response.data;

//     // Replace placeholders in the HTML with dynamic values
//     htmlContent = htmlContent
//       .replace('!username', username || 'Anonymous')
//       .replace('!avatarurl', avatarURL || 'https://s3.ap-northeast-1.amazonaws.com/anifie.tokyodome.resource/images/anifie_logo.webp')
//       .replace('!rank', `${rank || 0}`)
//       .replace('!rank_2', `${rank2 || 0}`)
//       .replace('!level', `${level || 0}`)
//       .replace('!total', `${current_total_points || 0}`)
//       .replace('!target', `${target || 0}`)
//       .replace('!percentage', `${(current_total_points / target) * 100 || 0}`)
//       //.replace('width: 37%', `width: ${(currentXP / totalXP) * 100 || 0}%`);

//     await page.setContent(htmlContent);
//     const screenshotBuffer = await page.screenshot();
//     await browser.close();

//     return screenshotBuffer;

//   } catch (error) {
//     console.log("error in generateRankCard", error);
//     throw error
//   } finally {
//     if (browser !== null) {
//       await browser.close();
//     }
//   }

// }

export const handler = async (event) => {
    
  console.log("event", event);


  //console.log("origin", headers['origin']);
  tableName = process.env.TABLE_NAME;
  // const domainProdArray = process.env.DOMAIN_PROD.split(',');
  // if (domainProdArray.some(domain => headers['origin'] === domain)) {
  //     tableName = process.env.TABLE_NAME;
  // }
  console.log("tableName", tableName);

  let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
  configs = configResult.Items.map(item => unmarshall(item));
  console.log("configs", configs);
  
  const PUBLIC_KEY = configs.find(x => x.key == 'DISCORD_PUBLIC_KEY').value;    // Your public key can be found on your application in the Developer Portal

  let body;

  try {
      
      
  //   // Ensure the Lambda function doesn't exit prematurely
  //   context.callbackWaitsForEmptyEventLoop = false;

    // Checking signature (requirement 1.)
    const signature = event.headers['x-signature-ed25519']
    const timestamp = event.headers['x-signature-timestamp'];
    // const strBody = event.body; // should be string, for successful sign
    const strBody = event.isBase64Encoded
                    ? Buffer.from(event.body, 'base64').toString('utf-8')
                    : event.body;

    const message = new TextEncoder().encode(timestamp + strBody);

    console.log("signature", signature);
    console.log("timestamp", timestamp);
    console.log("strBody", strBody);
    console.log("message", message);
    

    // Replying to ping (requirement 2.)
    body = JSON.parse(strBody)
    console.log('Raw body:', JSON.stringify(strBody));
    console.log('Encoded message:', message);

    if (body.type == 1) {
      
      if(!signature || !timestamp || !strBody) {
        return {
          statusCode: 401,
          body: JSON.stringify('invalid request signature'),
        };
      }

      const isVerified = nacl.sign.detached.verify(
        //Buffer.from(timestamp + strBody),
        message,
        Buffer.from(signature, 'hex'),
        Buffer.from(PUBLIC_KEY.trim(), 'hex')
      );

      if (!isVerified) {
        console.log('isVerified', isVerified, PUBLIC_KEY);        
        return {
          statusCode: 401,
          body: JSON.stringify('invalid request signature'),
        };
      }

      console.log("return type 1");
      
      return {
        statusCode: 200,
        body: JSON.stringify({ "type": 1 }),
      }

    }

    // Handle /foo Command
    if (body.type == 999999999) {
      
      // ping to keep-alive this lambda function

      return {
        statusCode: 200,
        body: JSON.stringify({ "type": 999999999 }),
      }

    }
    else if (body.data.name == 'accept' || (body.data.component_type == 2 && body.data.custom_id == 'ACCEPT')) {

      // Acknowledge the interaction
      await axios.post(`https://discord.com/api/v10/interactions/${body.id}/${body.token}/callback`, {
        type: 5, // DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE,
        data: {
          flags: 64
        }
      },{
        headers: {
            'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
        }
      });

      const isVerified = nacl.sign.detached.verify(
        Buffer.from(timestamp + strBody),
        Buffer.from(signature, 'hex'),
        Buffer.from(PUBLIC_KEY, 'hex')
      );
    
      if (!isVerified) {
        console.log("Interaction: ACCEPT. Signature error");
        return {
          statusCode: 401,
          body: JSON.stringify('invalid request signature'),
        };
      }

      console.log("body", body);

      // // grant reaction role to user
      // const GUILD_ID = process.env.DISCORD_GUILD_ID;
      // const BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;
      // const DISCORD_ROLE_ID_REACTION = process.env.DISCORD_ROLE_ID_REACTION;
      // const DISCORD_ROLE_ID_WALLET = process.env.DISCORD_ROLE_ID_WALLET;
      // const TARGET_CHANNEL_ID = process.env.DISCORD_REACTION_TARGET_CHANNEL_ID;
      // const GET_MEMBERSHIP_NFT_CHANNEL_ID = process.env.DISCORD_GET_MEMBERSHIP_NFT_CHANNEL_ID

      // //check user have the wallet role
      // let url = `https://discord.com/api/v10/guilds/${GUILD_ID}/members/${body.member.user.id}`
      // console.log('check role url', url);

      // let _headers = {
      //     "Authorization": `Bot ${BOT_TOKEN}`,
      //     "Content-Type": "application/json"
      // };
      
      // let checkRoleResult = await axios.get(url,
      //                                     {
      //                                         headers: _headers,
      //                                     });
      
      // console.log("checkRoleResult", checkRoleResult);

      // const member = checkRoleResult.data;

      // let message;

      // if (member.roles.includes(DISCORD_ROLE_ID_WALLET)) {
      //     console.log(`User has wallet role.`);
      //     // message = `に進んでください: <#${GET_MEMBERSHIP_NFT_CHANNEL_ID}>`;
      //     message = `<#${TARGET_CHANNEL_ID}> チャネルに移動しよう`;
      // } else {
      //     console.log(`User does not have wallet role.`);

      //     url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${body.member.user.id}/roles/${DISCORD_ROLE_ID_REACTION}`
      //     console.log('grant discord reaction role url', url);
          

      //     let grantRoleResult = await axios.put(url,
      //                                         null,
      //                                         {
      //                                             headers: _headers,
      //                                         });

      //     console.log("grant discord reaction role result", grantRoleResult);

      //     if(grantRoleResult.status != 204 && grantRoleResult.status != 200) {
      //         console.log('error granting discord role for reaction');
      //         // return {
      //         //     Sucess: false,
      //         //     Message: 'Discord ロールの付与エラー'
      //         // }

      //         // Send the follow-up message as ephemeral
      //         await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
      //           content: 'Discord ロールの付与エラー',
      //           flags: 64 // Make the message ephemeral
      //         },{
      //           headers: {
      //               'Authorization': `Bot ${process.env.DISCORD_BOT_TOKEN}`
      //           }
      //         });

      //     }
          
      //     message = `<#${TARGET_CHANNEL_ID}> チャネルに移動しよう`;
      // }


      let sql = `select * from "${tableName}"."InvertedIndex" where SK = '${body.member.user.id}' and type = 'DISCORD' and interaction_type = 'JOIN'`;
      console.log("sql", sql);
      let joinInteractionResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
      console.log("joinInteractionResult", JSON.stringify(joinInteractionResult));
      let interactionId;
      if(joinInteractionResult.Items.length === 0) {
        interactionId = ULID.ulid();
        await dbClient.send(new ExecuteStatementCommand({
              Statement: `INSERT INTO "${tableName}" 
                          VALUE { 'PK': ? , 'SK': ?, 'type': ?, 'interaction_id': ?, 'interaction_type': ?, 'created_date': ?, 'joined_date': ?}`,
              Parameters: [
                          { S: 'DISCORD#' + interactionId}, 
                          { S: body.member.user.id}, 
                          { S: 'DISCORD'}, 
                          { S: interactionId}, 
                          { S: 'JOIN'},
                          { S: new Date().toISOString()},
                          { S: new Date().toISOString()}
                          ],
        }));          
        console.log("inserted discord interaction");
        console.log("interactionId", interactionId);

        // Send the follow-up message as ephemeral
        await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
//           content: `以下のリンクを押して会員証を認証しよう！\n
// ※はじめにウェブサイトで認証に使用したSNSと同じSNSアカウントを使用してください。
// ※iOSでうまくいかない場合はPCもしくは他の端末でお試しください。\n
// ${configs.find(x => x.key == 'HOMEPAGE_URL').value}/discord/?guid=${interactionId}`,
          content: `Click the link below to authenticate your membership card!\n
※Please use the same social media account that you used to authenticate on the website initially.
※If it doesn't work on iOS, please try it on a PC or another device.\n
${configs.find(x => x.key == 'HOMEPAGE_URL').value}/discord/?guid=${interactionId}`,
          flags: 64 // Make the message ephemeral
        },{
          headers: {
              'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
          }
        });
      }
      else {
          let joinInteraction = joinInteractionResult.Items.map(unmarshall)[0];
          interactionId = joinInteraction.interaction_id;
          // if(!joinInteraction.joined_date) {
          //   let uptResult = await db.executeStatement({Statement: `UPDATE "${tableName}" SET joined_date = '${new Date().toISOString()}' WHERE PK = '${joinInteraction.PK}' AND SK = '${joinInteraction.SK}' `});
          //   console.log("uptResult", uptResult);
          // }
          console.log("joinInteraction", joinInteraction);
        
          if(joinInteraction.status == 'DONE') {
            // Send the follow-up message as ephemeral
            await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
              //content: `次のリンクをクリックしてウォレットを作成してください。\n・X（旧twitter）を使ったソーシャルログインがうまくいかない場合、google アカウントをお使いください\n・iOSでうまくいかない場合は、PCまたはほかの端末でお試しください \n ${process.env.HOMEPAGE_URL}/discord/?guid=` + interactionId, 
              // content: `既にウォレット作成済みです`,
              content: `Your wallet is already been linked to Discord account`,
              flags: 64
            },{
              headers: {
                  'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
              }
            });
          }
          else  {
            // Send the follow-up message as ephemeral
            await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
              //content: `次のリンクをクリックしてウォレットを作成してください。\n・X（旧twitter）を使ったソーシャルログインがうまくいかない場合、google アカウントをお使いください\n・iOSでうまくいかない場合は、PCまたはほかの端末でお試しください \n ${process.env.HOMEPAGE_URL}/discord/?guid=` + interactionId, 
//               content: `以下のリンクを押して会員証を認証しよう！\n
// ※はじめにウェブサイトで認証に使用したSNSと同じSNSアカウントを使用してください。
// ※iOSでうまくいかない場合はPCもしくは他の端末でお試しください。\n
// ${configs.find(x => x.key == 'HOMEPAGE_URL').value}/discord/?guid=${interactionId}`,
          content: `Click the link below to authenticate your membership card!\n
※Please use the same social media account that you used to authenticate on the website initially.
※If it doesn't work on iOS, please try it on a PC or another device.\n
${configs.find(x => x.key == 'HOMEPAGE_URL').value}/discord/?guid=${interactionId}`,
              flags: 64
            },{
              headers: {
                  'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
              }
            });

          }
          
          
      }
    } 
    // else if (body.data.name == 'xp' || (body.data.component_type == 2 && body.data.custom_id == 'xp')) {

    //   // Acknowledge the interaction
    //   await axios.post(`https://discord.com/api/v10/interactions/${body.id}/${body.token}/callback`, {
    //     type: 5, // DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE,
    //     data: {
    //       flags: 64
    //     }
    //   },{
    //     headers: {
    //         'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
    //     }
    //   });

    //   const isVerified = nacl.sign.detached.verify(
    //     Buffer.from(timestamp + strBody),
    //     Buffer.from(signature, 'hex'),
    //     Buffer.from(PUBLIC_KEY, 'hex')
    //   );
    
    //   if (!isVerified) {
    //     console.log("Interaction: XP. Signature error");
    //     return {
    //       statusCode: 401,
    //       body: JSON.stringify('invalid request signature'),
    //     };
    //   }

    //   console.log("body", body);

    //   console.log("body.data.custom_id", body.data.custom_id);
    //   console.log("body.member.user.id", body.member.user.id);

    //   let sql = `select * from "${tableName}"."InvertedIndex" where type = 'LEADERBOARD' and SK = '${body.member.user.id}'`;
    //   let leaderBoardResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
    //   console.log("leaderBoardResult", JSON.stringify(leaderBoardResult));

    //   if(leaderBoardResult.Items.length === 0) { 

    //     console.log('Leaderboard not found for discord user ' + body.member.user.id)

    //     // Send the follow-up message as ephemeral
    //     await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
    //       content: 'XPは取得されていません。',
    //       flags: 64 // Make the message ephemeral
    //     },{
    //       headers: {
    //           'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
    //       }
    //     });

    //   }
    //   else {

    //     let leaderBoard = leaderBoardResult.Items.map(unmarshall)[0];
        

    //     // const username = 'Its a Me';
    //     // const discriminator = '3546';
    //     // const avatarURL = 'https://i.pravatar.cc/150?u=' + body.member.user.id;
    //     // const rank = 26;
    //     // const level = 37;
    //     // const currentXP = 3340;
    //     // const totalXP = 8790;
        
    //     // Generate the rank card
    //     //const imageBuffer = await generateRankCard(username, discriminator, avatarURL, rank, level, currentXP, totalXP);
    //     // const imageBuffer = await generateRankCard(username, avatarURL, rank, level, currentXP, totalXP)

    //     let _member;
    //     sql = `select * from "${tableName}"."InvertedIndex" where PK = 'MEMBER#${leaderBoard.user_id}' and SK = 'MEMBERWALLET#${leaderBoard.wallet_address}' and type = 'MEMBER'`;
    //     console.log("sql", sql);
    //     let _memberResult = await fetchAllRecords(sql)
    //     console.log("_memberResult", _memberResult);        
    //     if(_memberResult.length > 0) {
    //       _member = _memberResult.map(unmarshall)[0];
    //       console.log("_member", _member);
    //     }

    //     // let nftImageURL;
    //     // if(_member && _member.nft_202412_token_id && _member.nft_202412_contract_address){ 
    //     //   sql = `select * from "${process.env.TABLE_NAME_COMMUNITY}" where type = 'ASSET' and PK = 'ASSET#${_member.nft_202412_contract_address}#${_member.nft_202412_token_id}' and SK = '${_member.SK}'`;
    //     //   console.log("sql", sql);
    //     //   let assetResult = await fetchAllRecords(sql);
    //     //   console.log("assetResult", assetResult);
    //     //   if(assetResult.length > 0){
    //     //     let asset = assetResult.map(unmarshall)[0];
    //     //     console.log("asset", asset);
    //     //     nftImageURL = asset.asset_url;
    //     //   }
    //     // } 
        
    //     const imageBuffer = await generateRankCard(leaderBoard.discord_user_id_real ? leaderBoard.discord_user_id_real : leaderBoard.discord_user_id, //leaderBoard.discord_user_name, 
    //                                                 getDiscordAvatarUrl(leaderBoard.discord_user_id, leaderBoard.discord_user_avatar, leaderBoard.discord_user_discriminator), //nftImageURL, //"https://s3.ap-northeast-1.amazonaws.com/anifie.community.resource/images/synergy-lab-logo.png",  
    //                                                 leaderBoard.rank, 
    //                                                 leaderBoard.rank_2, 
    //                                                 leaderBoard.level, 
    //                                                 leaderBoard.total_points, 
    //                                                 parseInt(leaderBoard.total_points) + parseInt(leaderBoard.points_required_to_next_level));

    //     // Save the image temporarily 
    //     const filePath = '/tmp/rank-card.png';  // tmp is aws lambda temporary storage
    //     fs.writeFileSync(filePath, imageBuffer);
        
    //     // FormData to send the image
    //     const form = new FormData();
    //     form.append(
    //         'payload_json',
    //         JSON.stringify({
    //             type: 4, // Channel message with source
    //             data: {
    //                 content: 'Here is your rank card!',
    //                 flags: 64, // Makes the message ephemeral
    //                 attachments: [
    //                     {
    //                         id: 0,
    //                         filename: 'rank-card.png',
    //                         description: 'Your rank card',
    //                     },
    //                 ],
    //             },
    //         })
    //     );
    //     form.append('files[0]', fs.createReadStream(filePath), 'rank-card.png');
        
    //     // Send the follow-up message as ephemeral
    //     // await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
    //     //   content: `Total Points : ${leaderBoard.total_points}\nLevel : ${leaderBoard.level}\nRank : ${leaderBoard.rank}`,  //\nPoints Required to Next Level : ${leaderBoard.points_required_to_next_level}
    //     //   flags: 64 // Make the message ephemeral
    //     // },{
    //     //   headers: {
    //     //       'Authorization': `Bot ${process.env.DISCORD_BOT_TOKEN}`
    //     //   }
    //     // });

    //     await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, form, {
    //       headers: {
    //         ...form.getHeaders(),
    //         'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
    //       }
    //     });

    //   }
      
    // }
      

    // return {
    //   statusCode: 404  // If no handler implemented for Discord's request
    // }

    console.log("return success");

    return {
      statusCode: 200,
      body: JSON.stringify({ received: true }),
    };

  } catch (e) {
    console.error('error in ch-discord-bot-test', e);

    const message = {
        Subject: 'TD Error - ch-discord-bot-test',
        Message: `Error in ch-discord-bot-test: ${e.message}\n\nStack trace:\n${e.stack}`,
        TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
    };
    
    const publishCommand = new PublishCommand(message);
    await snsClient.send(publishCommand);
    
    // return JSON.stringify({
    //   "type": 4, 
    //   "data": { 
    //             "content": "短期間に大量のユーザがボットにアクセスすると、Discordの仕様で、エラーが発生することがあります。エラーが解消しない場合は、しばらくしてから再度お試しください。", 
    //             "flags": '01000000' 
    //           }
    // })

    // Send an error follow-up message as ephemeral
    await axios.post(`https://discord.com/api/v10/webhooks/${body.application_id}/${body.token}`, {
      content: "短期間に大量のユーザがボットにアクセスすると、Discordの仕様で、エラーが発生することがあります。エラーが解消しない場合は、しばらくしてから再度お試しください。",
      flags: 64 // Make the message ephemeral
    },{
      headers: {
          'Authorization': `Bot ${configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value}`
      }
    });

    return {
      statusCode: 200,
      body: JSON.stringify({ received: true }),
    };

    // return {
    //   statusCode: 404  // If no handler implemented for Discord's request
    // }
  }

};
  