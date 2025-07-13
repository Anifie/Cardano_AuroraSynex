import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import axios from 'axios';
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 8 * 60 * 1000 // 1 minutes in milliseconds
    }
});

let tableName;
let configs;

const fileUpload = async (params) => {
    console.log("fileUpload", params);

    let lambdaParams = {
        FunctionName: 'ch-file-upload-post',
        InvocationType: 'RequestResponse', 
        LogType: 'Tail',
        Payload: {
            body: JSON.stringify({
                S3URL: configs.find(x => x.key == 'S3_URL').value,
                S3BucketName: configs.find(x => x.key == 'S3_BUCKET').value, 
                SNSTopic: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value, 
                assetId: params.assetId, fileData: params.fileData, fileName: params.fileName, fileExtension: params.fileExtension, params: params.params, isBase64: params.isBase64, isTest: params.isTest})
        }
    };
    // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
    // console.log("lambdaParams", lambdaParams);            
    // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
    // const uploaded = JSON.parse(lambdaResult.Payload).Success;    
    // if(lambdaResult.Payload.errorMessage) {
    //     console.log("file upload lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
    //     throw new Error('fileupload Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
    // }            
    // const uploadResult = JSON.parse(lambdaResult.Payload);    
    // console.log("uploadResult", uploadResult);

    // if(uploadResult) {
    //     return JSON.parse(uploadResult.body);
    // }

    try {
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        console.log("payload", payload);

        if (payload.errorMessage) {
            console.error("Upload file lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('Upload file Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        return JSON.parse(payload.body);

    } catch (error) {
        console.error("Upload file Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
    }
}

async function getImageBase64(url) {
    try {
      // Fetch the image data from the URL
      const response = await axios.get(url, {
        responseType: 'arraybuffer' // Important to get the data as a buffer
      });
  
      // Convert the response data to a Buffer
      const buffer = Buffer.from(response.data, 'binary');
  
      // Encode the Buffer to a Base64 string
      const base64String = buffer.toString('base64');
  
      // Optionally, you can add the data URI prefix to the Base64 string
      const mimeType = response.headers['content-type'];
      const base64Image = `data:${mimeType};base64,${base64String}`;
  
      return base64Image;
    } catch (error) {
      console.error('Error fetching and converting image:', error);
      throw error;
    }
}

const folderUpload = async (params) => {
    let lambdaParams = {
        FunctionName: 'ch-nft-folder-upload-post2',
        InvocationType: 'RequestResponse', 
        LogType: 'Tail',
        Payload: {
            artworkIdV1: params.artworkIdV1,
            artworkIdV2: params.artworkIdV2,
            isTest: params.isTest
        }
    };            

    try {
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        
        if (payload.errorMessage) {
            console.error("Upload folder2 lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('Upload folder2 Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        console.log("upload folder2 result", payload);
        return payload;
    } catch (error) {
        console.error("Upload folder2 Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
    }
}



export const handler = async (event) => {
    
    console.log("nft upgrade event", event);
    
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
        
        let memberId = null;
        let member;
        let sql;
        var token = headers['authorization'];
        console.log("token", token);

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

            sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBER_ID#${memberId}' and type = 'MEMBER' and begins_with("PK", 'MEMBER#')`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
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
            console.log('Missing required field');
            const response = {
                    Success: false,
                    Message: "Missing required field"
                };
            return response;
        }


        let asset;

        if(body.appPubKey) {

            // // called from member site

            if(body.unit == undefined) {
                return {
                    Success: false,
                    Message: "unit is required"
                };
            }

            let assetsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE PK = 'ASSET#${body.unit}' and SK = '${member.SK}' and type = 'ASSET'`}));
            console.log("assetsResult", JSON.stringify(assetsResult));
            if(assetsResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'NFT not found. unit : ' + body.unit
                };
            }

            asset = assetsResult.Items.map(unmarshall)[0];

        }
        else if (token){

            // called from admin portal only

            if(body.unit == undefined) {
                return {
                    Success: false,
                    Message: "unit is required"
                };
            }

            let assetsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ASSET#${body.unit}' and type = 'ASSET'`}));
            console.log("assetsResult", JSON.stringify(assetsResult));
            if(assetsResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'NFT not found. unit : ' + body.unit
                };
            }

            asset = assetsResult.Items.map(unmarshall)[0];

            // replace member with asset's owner
            sql = `select * from "${tableName}" where PK = 'MEMBER#${asset.owner_user_id}' and type = 'MEMBER'`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + asset.owner_user_id);
                const response = {
                    Success: false,
                    Message: "member not found: " + asset.owner_user_id
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];

        }

        if(body.queueId) {
            let sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , status = 'IN_PROGRESS' where PK = 'QUEUE#UPGRADE#${body.queueId}' and SK = '${member.SK}'`;
            let updateQueueInProgressResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("updateQueueInProgressResult", updateQueueInProgressResult);
        }

        let isDoubleSided = asset.local_url.includes(',');
        console.log('isDoubleSided', isDoubleSided);

        let metadata = JSON.parse(asset.metadata);
        let rank = metadata.attributes.find(x => x.trait_type == 'Rank').value;
        let rarity = metadata.attributes.find(x => x.trait_type == 'Rarity').value;
        let community = metadata.attributes.find(x => x.trait_type == 'Community').value;
        let title = metadata.attributes.find(x => x.trait_type == 'Title').value;

        console.log("metadata", metadata);
        console.log("rank", rank);
        console.log("rarity", rarity);
        console.log("community", community);
        console.log("title", title);

        let action, newRank, whitelistType, artworkId, artworkIdV2, discordRoleId, discordRoleName;
        switch(rank) {
            case 'Bronze':
                action = 'UPGRADE_MEMBERSHIP_SILVER';
                newRank = 'Silver';
                if(rarity == 'Legend') {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_CAMPAIGN_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_CAMPAIGN_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_SILVER";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_CAMPAIGN_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_CAMPAIGN_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE);
                            discordRoleName = "METAGARAGE_SILVER";
                            break;
                    }
                }
                else if(rarity == 'Common' && title == 'Innovator') {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_PRE_REGISTER_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_PRE_REGISTER_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_SILVER";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_PRE_REGISTER_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_PRE_REGISTER_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE);
                            discordRoleName = "METAGARAGE_SILVER";
                            break;
                    }
                }
                else if(rarity == 'Common' && (title == 'Assosiate' || title == 'Associate')) {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_POST_REGISTER_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_POST_REGISTER_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_SILVER_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_SILVER";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_POST_REGISTER_SILVER_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_POST_REGISTER_SILVER_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_SILVER_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_SILVER_METAGARAGE);
                            discordRoleName = "METAGARAGE_SILVER";
                            break;
                    }
                }
                break;
            case 'Silver':
                action = 'UPGRADE_MEMBERSHIP_GOLD';
                newRank = 'Gold';
                if(rarity == 'Legend') {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_CAMPAIGN_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_CAMPAIGN_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_GOLD";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_CAMPAIGN_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_CAMPAIGN_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE);
                            discordRoleName = "METAGARAGE_GOLD";
                            break;
                    }
                }
                else if(rarity == 'Common' && title == 'Innovator') {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_PRE_REGISTER_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_PRE_REGISTER_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_GOLD";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_PRE_REGISTER_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_PRE_REGISTER_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE);
                            discordRoleName = "METAGARAGE_GOLD";
                            break;
                    }
                }
                else if(rarity == 'Common' && (title == 'Assosiate' || title == 'Associate')) {
                    switch (community) {
                        case 'PaleBlueDot.':
                            artworkId = process.env.MEMBER_A_POST_REGISTER_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_A_POST_REGISTER_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_PALEBLUEDOT';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT_TEST : process.env.DISCORD_ROLE_ID_GOLD_PALEBLUEDOT);
                            discordRoleName = "PALEBLUEDOT_GOLD";
                            break;
                        case 'MetaGarage':
                            artworkId = process.env.MEMBER_B_POST_REGISTER_GOLD_ARTWORK_ID;
                            artworkIdV2 = isDoubleSided ?  process.env.MEMBER_B_POST_REGISTER_GOLD_ARTWORK_ID_V2 : undefined;
                            whitelistType = 'WHITELIST_MEMBER_GOLD_METAGARAGE';
                            discordRoleId = (tableName == process.env.TABLE_NAME_TEST ? process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE_TEST : process.env.DISCORD_ROLE_ID_GOLD_METAGARAGE);
                            discordRoleName = "METAGARAGE_GOLD";
                            break;
                    }
                }
                break;
            default:
                console.log("rank", rank);
                console.log("You cannot upgrade to a Gold membership or higher because you already have a Gold membership");
                return {
                    Success: false,
                    Message: "すでにゴールド メンバーシップを持っているため、ゴールド メンバーシップ以上にアップグレードすることはできません"
                };
        }

        console.log("action", action);
        console.log("artworkId", artworkId);
        console.log("artworkIdV2", artworkIdV2);
        console.log("newRank", newRank);

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = '${whitelistType}'`;
        console.log("sql", sql);
        let whiteListResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(whiteListResult.Items.length == 0) {
            console.log('Member is not part of ' + whitelistType);
            let msg;
            if(whitelistType.includes('_GOLD_'))
                msg = 'あなたにはゴールド会員資格がありません'; //  You are not eligible for Gold Membership
            else if(whitelistType.includes('_SILVER_'))
                msg = 'あなたにはシルバー会員資格がありません'; // You are not eligible for Silver Membership
            else 
                msg = 'メンバーシップをアップグレードする資格がありません'; // You are not eligible to upgrade Membership

            return {
                Success: false,
                Message: msg
            }
        }

        let newAttributes = [];
        for (let i = 0; i < metadata.attributes.length; i++) {
            const attr = metadata.attributes[i];
            if(attr.trait_type != 'Rank')
                newAttributes.push(attr);
            else
                newAttributes.push({"trait_type": "Rank", "value": newRank})
        }
        metadata.attributes = newAttributes;
        

        console.log("after modified metadata", metadata);

        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${artworkId}'`;
        let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        let artwork = artworkResult.Items.map(unmarshall)[0];
        let imgBase64 = await getImageBase64(artwork.two_d_url);
        
        let artworkV2;  //, imgBase64V2;

        if(artworkIdV2) {
            sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${artworkIdV2}'`;
            let artworkV2Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            artworkV2 = artworkV2Result.Items.map(unmarshall)[0];
            // imgBase64V2 = await getImageBase64(artworkV2.two_d_url);
        }

        // let cidNFTFile;
        let arMetadataUploadResult;

        if(artwork && artworkV2) {

            // let cidNFTFolder = await folderUpload({
            //     artworkIdV1: artwork.artwork_id,
            //     artworkIdV2: artworkV2.artwork_id
            // });

            let uploadResultNFTFolder = await folderUpload({
                artworkIdV1: artwork.artwork_id,
                artworkIdV2: artworkV2.artwork_id,
                isTest: tableName == process.env.TABLE_NAME_TEST
            });

            // if(cidNFTFolder.errorMessage) {
            //     console.log('upload folder err', cidNFTFolder);
            //     return {
            //         Success: false,
            //         Message: 'Upload folder failed'
            //     }
            // }

            if(uploadResultNFTFolder.errorMessage) {
                console.log('upload folder err', uploadResultNFTFolder);
                return {
                    Success: false,
                    Message: 'Upload folder failed'
                }
            }

            let _meta = {
                name: metadata.name,
                //image: `ipfs://${cidNFTFolder}/images/v1/${artwork.two_d_file_name}`,
                image: `https://arweave.net/${uploadResultNFTFolder.img1TxId}`,
                //animation_url: `ipfs://${cidNFTFolder}/index.html?id=${artwork.two_d_file_name}`,
                //animation_url: `https://${cidNFTFolder}.ipfs.nftstorage.link/?id=${artwork.two_d_file_name}`,
                //animation_url: `https://${cidNFTFolder}.ipfs.nftstorage.link/index.html`,
                animation_url: `https://arweave.net/${uploadResultNFTFolder.htmlTxId}`,
                description: metadata.description,
                publisher: metadata.publisher,
                attributes: metadata.attributes
            }
            console.log("_meta", _meta);

            metadata = _meta
            

            // let _metaBuffer = Buffer.from(JSON.stringify(_meta));
            // let _metaBase64 = _metaBuffer.toString('base64')

            // console.log("_metaBase64", _metaBase64);

            // arMetadataUploadResult = await fileUpload({
            //                                         isBase64: true,
            //                                         fileData: _metaBase64,
            //                                         fileName: 'metadata.json',
            //                                         fileExtension: 'json',
            //                                         isTest: tableName == process.env.TABLE_NAME_TEST
            //                                     });

            // arMetadataUploadResult.localURL = artwork.two_d_url + "," + artworkV2.two_d_url;

        }
        else {

            let arTxId;
            if(artwork.ar_tx_id) {
                arTxId = artwork.ar_tx_id;
            }
            else {
                let arImageUploadResult = await fileUpload({
                    assetId: asset.asset_id,
                    isBase64: true,
                    fileData: imgBase64.split(',').pop(),
                    fileName: artwork.two_d_file_name,
                    fileExtension: artwork.two_d_file_name.split('.').pop(),
                    isTest: tableName == process.env.TABLE_NAME_TEST
                })

                console.log("arImageUploadResult", arImageUploadResult);
                
                arTxId = arImageUploadResult.metadata.transaction.id;

                if(artwork) {
                    sql = `update "${tableName}" set ar_tx_id = '${arTxId}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
                    let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                    console.log("updateArtworkResult", updateArtworkResult);
                }
            }

            metadata.image = `https://arweave.net/${arTxId}`;            
            console.log("metadata", metadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(metadata));
            // let _metaBase64 = _metaBuffer.toString('base64')
            // console.log("_metaBase64", _metaBase64);

            // arMetadataUploadResult = await fileUpload({
            //     isBase64: true,
            //     fileData: _metaBase64,
            //     fileName: 'metadata.json',
            //     fileExtension: 'json',
            //     isTest: tableName == process.env.TABLE_NAME_TEST
            // });

            // arMetadataUploadResult.localURL = artwork.two_d_url;
        }
        
        
        // console.log("arMetadataUploadResult", arMetadataUploadResult)

        // cidNFTFile = JSON.parse(cidNFTFile);


        // let retryCount = 0;
        // let maxRetry = 5;
        // while (true) {
        //     try {
        //         let metadataResponse = await axios.get(cidNFTFile.metadata.url.replace("ipfs://", `${process.env.IPFS_GATEWAY_DOMAIN}/ipfs/`))
        //         metadata = await metadataResponse.data;
        //         console.log("metadata", metadata)
        //         break; 
        //     } catch (error) {
                
        //         retryCount++;
                
        //         console.log("Failed to get metadata", retryCount, error);

        //         if(retryCount >= maxRetry) 
        //             throw error;
        //         else 
        //             console.log('retry');   
        //     }
        // }


        // call meshsdk to update metadata
        let lambdaParams = {
            FunctionName: 'ch-web3',
            InvocationType: 'RequestResponse', 
            LogType: 'Tail',
            Payload: {
                action: 'UPDATE_MEMBERSHIP_METADATA',
                unit: asset.unit,
                metadata: metadata,
                isTest: tableName == process.env.TABLE_NAME_TEST
            }
        };
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        let upgradeResult = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        console.log("upgradeResult", upgradeResult);
        if (upgradeResult.errorMessage) {
            console.error("ambda error message:", JSON.stringify(upgradeResult.errorMessage));
            throw new Error('Lambda error: ' + JSON.stringify(upgradeResult.errorMessage));
        }
        if(upgradeResult.transactionHash != undefined) {
            console.log("upgraded nft. txHash: "  + upgradeResult.transactionHash);

            let txStatements = [];

            // update asset metadata
            sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' `;
            if(action == 'UPGRADE_MEMBERSHIP_SILVER') {
                sql += `, is_silver = true , upgrade_silver_transaction_hash = '${upgradeResult.transactionHash}' `;
            }
            else if (action == 'UPGRADE_MEMBERSHIP_GOLD') {
                sql += `, is_gold = true , upgrade_gold_transaction_hash = '${upgradeResult.transactionHash}' `;
            }

            sql += ` , asset_url = '${metadata.image}' 
                    , asset_thumbnail_url = '${metadata.image}' 
                    , nft_url = '${metadata.animation_url ? metadata.animation_url : metadata.image}' 
                    , metadata = ? 
                     where PK = '${asset.PK}' and SK = '${asset.SK}'`;

            txStatements.push({ "Statement": sql, Parameters: [{S: JSON.stringify(metadata)}]});

            const statements = { "TransactStatements": txStatements };
            console.log("statements", JSON.stringify(statements));
            
            const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
            console.log("updateUpgradeResult", dbTxResult);

            if(body.queueId) {
                let _upgradeResult = {
                    transactionHash: upgradeResult.transactionHash
                }
                sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , result = '${JSON.stringify(_upgradeResult)}' where PK = 'QUEUE#UPGRADE#${body.queueId}' and SK = '${member.SK}'`;
                let updateQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("updateQueueResult", updateQueueResult);
            }
            
            return {
                Success: true,
                Data: {
                    transactionHash: upgradeResult.transactionHash
                }
            }
        }
        else {
            console.log('Failed to upgrade NFT in blockchain');
            return {
                Success: false,
                Message: "Failed to upgrade NFT in blockchain"
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-upgrade-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-upgrade-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-upgrade-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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