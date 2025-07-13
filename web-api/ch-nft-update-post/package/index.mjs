import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import crypto from 'crypto';
import { MerkleTree } from 'merkletreejs';
import OpenAI from 'openai'

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

const fetchTextFileAsWordsArray = async (url, openAiApiKey) => {
    try {
        // Fetch the content of the text file
        const response = await axios.get(url, { responseType: "text" });

        // Ensure the response contains text data
        if (response.headers['content-type']?.includes('text/plain')) {
            const textContent = response.data;

            // Initialize OpenAI API client
            const openai = new OpenAI({
                                            apiKey: openAiApiKey
                                        });

            const aiResponse = await openai.chat.completions.create({
                model: "gpt-3.5-turbo", //"gpt-4",
                messages: [
                  { role: "system", content: `以下の日本語の文章を意味のある単語ごとに分割してください。それぞれの単語は配列の要素として出力されます。各単語は文脈上の意味を保ってください。 出力例: ["単語1", "単語2", "単語3", ...]` }, 
                  { role: "user", content: `文章: ${textContent}` },
                ],
              });
            
            console.log("aiResponse", aiResponse);              

            // Parse and return the result
            const wordsArray = JSON.parse(aiResponse.choices[0].message.content.trim());
            console.log("Array of Words:", wordsArray);
            return wordsArray;
        } else {
            throw new Error("The URL does not contain a plain text file.");
        }
    } catch (error) {
        console.error("Error processing the text file:", error.message);
        throw error;
    }
};


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
                assetId: params.assetId, fileData: params.fileData, fileName: params.fileName, fileExtension: params.fileExtension, params: params.params, isBase64: params.isBase64, isTest: params.isTest, isURL: params.isURL, skipNFTStorage: params.skipNFTStorage})
        }
    };

    try {
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        
        if (payload.errorMessage) {
            console.error("file upload lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('file upload Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        const uploadResult = JSON.parse(Buffer.from(lambdaResult.Payload).toString()); 
        console.log("uploadResult", uploadResult);

        if(uploadResult) {
            return JSON.parse(uploadResult.body);
        }

    } catch (error) {
        console.error("file upload Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
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
            console.error("folder upload lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('folder upload Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        console.log("uploadResult", payload);

        return payload;

    } catch (error) {
        console.error("folder upload Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
    }

    // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
    // console.log("lambdaParams", lambdaParams);            
    // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
    // console.log("lambdaResult", lambdaResult);            
    // if(lambdaResult.Payload.errorMessage) {
    //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
    //     throw new Error('Upload folder Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
    // }            
    // let cid = JSON.parse(lambdaResult.Payload);    
    // console.log("upload folder result", cid);
    // return cid;
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

// const folderUpload = async (params) => {
//     let lambdaParams = {
//         FunctionName: 'td-nft-folder-upload-post2',
//         InvocationType: 'RequestResponse', 
//         LogType: 'Tail',
//         Payload: {
//             artworkIdV1: params.artworkIdV1,
//             artworkIdV2: params.artworkIdV2,
//             isTest: params.isTest
//         }
//     };            
//     lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
//     console.log("lambdaParams", lambdaParams);            
//     const lambdaResult = await lambda.invoke(lambdaParams).promise();            
//     console.log("lambdaResult", lambdaResult);            
//     if(lambdaResult.Payload.errorMessage) {
//         console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
//         throw new Error('Upload folder Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
//     }            
//     let cid = JSON.parse(lambdaResult.Payload);    
//     console.log("upload folder result", cid);
//     return cid;
// }


// function encryptFile(jsonData, password) {
//     if (password.includes(';')) {
//         throw new Error('Semicolon is not allowed in the file password.');
//     }

//     // Convert password to a Buffer
//     const passwordBuffer = Buffer.from(password, 'utf-8');

//     // Generate salt and IV
//     const salt = crypto.randomBytes(16);
//     const iv = crypto.randomBytes(16);

//     // Derive key using PBKDF2
//     const key = crypto.pbkdf2Sync(passwordBuffer, salt, 100000, 32, 'sha256');

//     // Encrypt data using AES-GCM
//     const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
//     const jsonBuffer = Buffer.from(JSON.stringify(jsonData), 'utf-8');
//     const encryptedData = Buffer.concat([cipher.update(jsonBuffer), cipher.final()]);
//     const authTag = cipher.getAuthTag();

//     // Combine salt, IV, authTag, and encrypted data into one file
//     const combinedData = Buffer.concat([salt, iv, authTag, encryptedData]);

//     return {
//         combinedData,
//         salt: salt.toString('hex'),
//         iv: iv.toString('hex'),
//         authTag: authTag.toString('hex'),
//     };
// }


async function encryptFile(fileBuffer, filePassword) {
    if (filePassword.includes(';')) {
        throw new Error('Semicolon is not allowed in file password');
    }

    // Convert password to a buffer
    const passwordBuffer = Buffer.from(filePassword, 'utf-8');

    // Generate salt and IV
    const salt = crypto.randomBytes(16);
    const iv = crypto.randomBytes(16);

    // Encode salt and IV as hexadecimal strings
    const saltHex = salt.toString('hex');
    const ivHex = iv.toString('hex');

    // Derive a key using PBKDF2
    const key = await new Promise((resolve, reject) => {
        crypto.pbkdf2(passwordBuffer, salt, 100000, 32, 'sha256', (err, derivedKey) => {
            if (err) reject(err);
            resolve(derivedKey);
        });
    });

    // Encrypt the file data
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const encryptedData = Buffer.concat([cipher.update(fileBuffer), cipher.final()]);
    const authTag = cipher.getAuthTag(); // GCM authentication tag

    return {
        encryptedData,
        salt: saltHex,
        iv: ivHex,
        authTag: authTag.toString('hex'),
    };
}

const getCurrentDateInJST = () => {
    const date = new Date();
  
    const options = {
      timeZone: 'Asia/Tokyo',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    };
  
    const formattedDate = new Intl.DateTimeFormat('ja-JP', options).format(date);
  
    // Replace Japanese era slashes with regular ones
    return formattedDate.replaceAll('.', '/');
};

const hashData = (data) => {
  return crypto.createHash('sha256').update(data).digest();
};

export const handler = async (event) => {
    
    console.log("nft update event", event);
    
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

        if(token && !body.appPubKey) {
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

            if(!member.role?.includes('ADMIN')) {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }

        }
        else if(body.appPubKey) {

            token = headers['authorization'];
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

        // let contractAddress;
        // let tokenId;

        if(!body.unit) {
            return {
                Success: false,
                Message: "unit is required"
            }
        }

        if(!body.nftType) {
            return {
                Success: false,
                Message: "nftType is required"
            }
        }

        if(body.nftType != 'CAR' && body.nftType != 'CHARACTER' && body.nftType != 'MEMBER_A' && body.nftType != 'MEMBER_B' && body.nftType != 'RACINGFAN') {
            return {
                Success: false,
                Message: 'Invalid nftType'
            }
        }

        if(!body.metadata) {
            // return {
            //     Success: false,
            //     Message: "metadata is required"
            // }
        }

        // if(!body.artistCode) {
        //     return {
        //         Success: false,
        //         Message: 'artistCode is required'
        //     }
        // }

        let action;

        switch(body.nftType) {
            case "CAR":
                action = 'UPDATE_CAR_METADATA';
                break;
            case "CHARACTER":
                action = 'UPDATE_CHARACTER_METADATA';
                break;
            case "MEMBER_A":
            case "MEMBER_B":
                action = 'UPDATE_MEMBERSHIP_METADATA';
                break;
            case "RACINGFAN":
                action = 'UPDATE_RACINGFAN_METADATA';
                break;
        }

        let asset;
        let sql;

        if(body.appPubKey) {

            // // called from member site

            // let discordInteractionResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'DISCORD#${body.interactionId}' and type = 'DISCORD' and interaction_type = 'REVEAL'`}).promise();
            // console.log("discordInteractionResult", JSON.stringify(discordInteractionResult));
            // if(discordInteractionResult.Items.length === 0) {
            //     return {
            //         Success: false,
            //         Message: 'Discord Interaction not found',
            //     };
            // }

            // let discordInteraction = discordInteractionResult.Items.map(unmarshall)[0];
            // let discordData = JSON.parse(discordInteraction.data);

            // contractAddress = discordData.split('#')[0];
            // tokenId = discordData.split('#')[1];

            if(body.unit == undefined) {
                return {
                    Success: false,
                    Message: "unit is required"
                };
            }

            let assetsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ASSET#${body.unit}' and SK = '${member.SK}' and type = 'ASSET'`}));
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

            // if(body.contractAddress == undefined) {
            //     return {
            //         Success: false,
            //         Message: "contractAddress is required"
            //     };
            // }

            // contractAddress = body.contractAddress;
            // tokenId = body.tokenId;

            let assetsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ASSET#${body.unit}' and type = 'ASSET'`}));
            console.log("assetsResult", JSON.stringify(assetsResult));
            if(assetsResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'NFT not found. contract address : unit : ' + body.unit
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
            let sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , status = 'IN_PROGRESS' where PK = 'QUEUE#UPDATE#${body.queueId}' and SK = '${member.SK}'`;
            let updateQueueInProgressResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("updateQueueInProgressResult", updateQueueInProgressResult);
        }

        let newMetadata;
        let memberWhiteLists = [];
        let txStatements = [];
        let chatDataLocalURL;
        let chatDataHash;
        let encryptionKey;

        if(body.nftType == 'CHATDATA') {
        
            let chatMemberId = body.chatMemberId;
            let msgsObj = [];
            let lastMintMessageId;
            body.metadata = {
                attributes: []
            };

            if(body.messages) {
                if(typeof body.messages == 'string') {
                    body.messages = JSON.parse(body.messages);
                }

                for (let i = 0; i < body.messages.length; i++) {
                    const messageObj = body.messages[i];
                    console.log("messageObj", messageObj);
                    /*
                    let msg = {
                                    DiscordUserId: obj.discord_user_id,
                                    ChannelId: obj.discord_channel_id,
                                    // ChannelName: obj.channel_name,
                                    Timestamp: obj.timestamp,
                                    MessageId: obj.discord_message_id,
                                    Message: obj.content
                                };
                    */
                    msgsObj.push({
                        type: "CHANNELMESSAGE",
                        channelId: messageObj.ChannelId,
                        messageId: messageObj.MessageId,
                        message: messageObj.Message,
                        createdDate: messageObj.Timestamp
                    });
                }

                const latestMessage = body.messages.reduce((latest, msg) => 
                    BigInt(msg.MessageId) > BigInt(latest.MessageId) ? msg : latest
                  );

                lastMintMessageId = latestMessage.MessageId;
                console.log("lastMintMessageId", lastMintMessageId);

                sql = `update "${tableName}" set last_mint_message_id = '${lastMintMessageId}' where PK = '${member.PK}' and SK = '${member.SK}'`;
                txStatements.push({ "Statement": sql});         
            }

            // if(body.messageIds) {
            //     let msgIds = body.messageIds.split(',');
            //     for (let i = 0; i < msgIds.length; i++) {
            //         const msgId = msgIds[i];
            //         let sql = `select * from "${tableName}"."InvertedIndex" where type = 'CHATCHANNEL_MESSAGE' and SK = 'MESSAGE#${msgId}' and sender_user_id = '${chatMemberId}'`;    
            //         let msgResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            //         if(msgResult.Items.length == 0) {
            //             throw new Error("Message not found: " + msgId);
            //         }
            //         let msg = msgResult.Items.map(unmarshall)[0];
            //         msgsObj.push({
            //             type: "CHANNELMESSAGE",
            //             channelId: msg.chat_channel_id,
            //             messageId: msg.message_id,
            //             message: msg.message,
            //             createdDate: msg.created_date
            //         });
            //     }
            // }

            // if(body.threadMessageIds) {
            //     let thMsgIds = body.threadMessageIds.split(',');
            //     for (let i = 0; i < thMsgIds.length; i++) {
            //         const thMsgId = thMsgIds[i];
            //         let sql = `select * from "${tableName}"."InvertedIndex" where type = 'CHATCHANNEL_THREADMESSAGE' and SK = 'THREADMESSAGE#${thMsgId}' and sender_user_id = '${chatMemberId}'`;    
            //         let thMsgResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            //         if(thMsgResult.Items.length == 0) {
            //             throw new Error("Thread Message not found: " + thMsgId);
            //         }
            //         let thMsg = thMsgResult.Items.map(unmarshall)[0];
            //         msgsObj.push({
            //             type: "THREADMESSAGE",
            //             channelId: thMsg.chat_channel_id,
            //             threadId: thMsg.thread_id,
            //             threadMessageId: thMsg.thread_message_id,
            //             message: thMsg.message,
            //             createdDate: thMsg.created_date
            //         });
            //     }
            // }

            // Create the Merkle Tree
            
            let _messages = msgsObj.map(x => x.message + ' |');
            let messages = _messages.join(' ').slice(0, -1);
            let messagesBuffer = Buffer.from(messages);
            let messagesBuffer64 = messagesBuffer.toString('base64')
            console.log("messagesBuffer64", messagesBuffer64);

            let textUploadResult = await fileUpload({
                assetId: body.chatMemberId + `_chatdata_${new Date().getTime()}`,
                isBase64: true,
                fileData: messagesBuffer64,
                fileName: body.chatMemberId + `_chatdata_${new Date().getTime()}.txt`,
                fileExtension: 'txt',
                isTest: tableName == process.env.TABLE_NAME_TEST,
                skipNFTStorage: true
            });

            console.log("textUploadResult", textUploadResult);

            chatDataLocalURL = textUploadResult.localURL;

            let wordsArray = await fetchTextFileAsWordsArray(chatDataLocalURL, configs.find(x => x.key == 'OPENAI_APIKEY').value);   

            // Create the Merkle Tree
            // const leaves = msgsObj.map(data => hashData(data));
            const leaves = wordsArray.map(data => hashData(data));
            const tree = new MerkleTree(leaves, (data) => crypto.createHash('sha256').update(data).digest());
            console.log('tree', tree);

            // Get the Merkle Root
            const root = tree.getRoot().toString('hex');
            console.log('Merkle Root:', root);

            chatDataHash = root;

            body.metadata.attributes.push({
                "trait_type": "ChatDataHash",
                "value": root
            });

            let filePassword = chatMemberId + '_pwd!';
            const jsonBuffer = Buffer.from(JSON.stringify(msgsObj), 'utf-8');
            let encryptResult = await encryptFile(jsonBuffer, filePassword);
            encryptionKey = `${filePassword};${encryptResult.salt};${encryptResult.iv};${encryptResult.authTag}`;
            
            let encyptedFileUploadResult = await fileUpload({
                                                                assetId: chatMemberId + "_chatdata",
                                                                isBase64: false,
                                                                fileData: encryptResult.encryptedData,
                                                                fileName: chatMemberId + '_chatdata.bin',
                                                                fileExtension: 'bin',
                                                                isTest: tableName == process.env.TABLE_NAME_TEST
                                                            });

            console.log("encyptedFileUploadResult", encyptedFileUploadResult);

            body.metadata.attributes.push({
                "trait_type": "ChatDataFile",
                "value": `https://arweave.net/${encyptedFileUploadResult.metadata.transaction.id}`
            });

        }

        if(body.metadata) {
            newMetadata = typeof body.metadata == 'string' ? JSON.parse(body.metadata) : body.metadata;
        }
        
        let originalMetadata = typeof asset.metadata == 'string' ? JSON.parse(asset.metadata) : asset.metadata;

        console.log("newMetadata", newMetadata);
        console.log("before originalMetadata", originalMetadata);

        if(newMetadata.name) 
            originalMetadata.name = newMetadata.name;

        if(newMetadata.description) 
            originalMetadata.description = newMetadata.description;

        if(newMetadata.termsOfService) 
            originalMetadata.termsOfService = newMetadata.termsOfService;

        if(newMetadata.publisher) 
            originalMetadata.publisher = newMetadata.publisher;

        if(newMetadata.attributes) {
            for (let i = 0; i < newMetadata.attributes.length; i++) {                
                const newAttr = newMetadata.attributes[i];
                if(originalMetadata.attributes.find(x=> x.trait_type == newAttr.trait_type))
                    originalMetadata.attributes.find(x=> x.trait_type == newAttr.trait_type).value = newAttr.value;
                else
                    originalMetadata.attributes.push(newAttr);
            }
        }

        console.log("after originalMetadata", originalMetadata);
            
        let artwork, artworkV2, imgBase64, imgBase64V2;

        if(body.artworkId && !body.artworkIdV2) {
            // single sided
            sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkId}'`;
            let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            artwork = artworkResult.Items.map(unmarshall)[0];
            imgBase64 = await getImageBase64(artwork.two_d_url);

        }
        else if(body.artworkId && body.artworkIdV2) {
            // double sided
            sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkId}'`;
            let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            artwork = artworkResult.Items.map(unmarshall)[0];
            imgBase64 = await getImageBase64(artwork.two_d_url);
            

            if(body.artworkIdV2) {
                sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkIdV2}'`;
                let artworkV2Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                artworkV2 = artworkV2Result.Items.map(unmarshall)[0];
                imgBase64V2 = await getImageBase64(artworkV2.two_d_url);
            }
        }

        let arMetadataUploadResult;
        
        if(artwork && artworkV2) {

            let uploadResultNFTFolder = await folderUpload({
                artworkIdV1: artwork.artwork_id,
                artworkIdV2: artworkV2.artwork_id,
                isTest: tableName == process.env.TABLE_NAME_TEST
            });

            if(uploadResultNFTFolder.errorMessage) {
                console.log('upload folder err', uploadResultNFTFolder);
                return {
                    Success: false,
                    Message: 'Upload folder failed'
                }
            }

            originalMetadata.image = `https://arweave.net/${uploadResultNFTFolder.img1TxId}`,
            originalMetadata.animation_url = `https://arweave.net/${uploadResultNFTFolder.htmlTxId}`;
            console.log("originalMetadata", originalMetadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(originalMetadata));
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
        else if(artwork && !artworkV2) {

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

                arTxId = arImageUploadResult.metadata.transaction.id;

                if(artwork) {
                    sql = `update "${tableName}" set ar_tx_id = '${arTxId}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
                    let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                    console.log("updateArtworkResult", updateArtworkResult);
                }
            }

            originalMetadata.image = `https://arweave.net/${arTxId}`;            
            console.log("originalMetadata", originalMetadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(originalMetadata));
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
        else if(!artwork && !artworkV2) {

            // console.log("originalMetadata", originalMetadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(originalMetadata));
            // let _metaBase64 = _metaBuffer.toString('base64')
            // console.log("_metaBase64", _metaBase64);

            // arMetadataUploadResult = await fileUpload({
            //     isBase64: true,
            //     fileData: _metaBase64,
            //     fileName: 'metadata.json',
            //     fileExtension: 'json',
            //     isTest: tableName == process.env.TABLE_NAME_TEST
            // });

            // arMetadataUploadResult.localURL = undefined;
        }
        
        console.log("arMetadataUploadResult", arMetadataUploadResult)

        // call smart contract to update metadata
        let lambdaParams = {
            FunctionName: 'ch-web3',
            InvocationType: 'RequestResponse', 
            LogType: 'Tail',
            Payload: {
                action: action,
                unit: asset.unit,
                metadata: originalMetadata,
                //uri: `https://arweave.net/${arMetadataUploadResult.metadata.transaction.id}`, //cidNFTFile.metadata.url
                isTest: tableName == process.env.TABLE_NAME_TEST,
                encryptionKey: encryptionKey,
                // artistCode: body.artistCode
            }
        };
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        let updateResult = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        console.log("updateResult", updateResult);
        if (updateResult.errorMessage) {
            console.error("ambda error message:", JSON.stringify(updateResult.errorMessage));
            throw new Error('Lambda error: ' + JSON.stringify(updateResult.errorMessage));
        }
        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
        // console.log("lambdaParams", lambdaParams);            
        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
        // console.log("lambdaResult", lambdaResult);            
        // if(lambdaResult.Payload.errorMessage) {
        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
        // }            
        // let updateResult = JSON.parse(lambdaResult.Payload);    
        // console.log("updateResult", updateResult);        
        if(updateResult.transactionHash != undefined) {
            console.log("update nft. txHash: "  + updateResult.transactionHash);

            // update asset metadata
            // , metadata_url = 'https://arweave.net/${arMetadataUploadResult.metadata.transaction.id}' 
            sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' 
                    , asset_url = '${originalMetadata.image}' 
                    , asset_thumbnail_url = '${originalMetadata.image}' 
                    , nft_url = '${originalMetadata.animation_url ? originalMetadata.animation_url : originalMetadata.image}' 
                    , metadata = ? `;
            
            // if(arMetadataUploadResult.localURL) {
            //     sql += `, local_url = '${arMetadataUploadResult.localURL}' `;
            // }
            
            if(body.artworkId && body.artworkIdV2) {
                sql += ` , media_type = 'text/html' `;
            }

            if(chatDataLocalURL) {
                sql += ` , chat_data_local_url = '${chatDataLocalURL}' `;
            }

            if(chatDataHash) {
                sql += ` , chat_data_hash = '${chatDataHash}' `;
            }

            if(encryptionKey) {
                sql += ` , encryption_key = '${encryptionKey}' `;
            }

            sql += ` where PK = '${asset.PK}' and SK = '${asset.SK}'`;

            txStatements.push({ "Statement": sql, Parameters: [{S: JSON.stringify(originalMetadata)}]});            

            const statements = { "TransactStatements": txStatements };
            console.log("statements", JSON.stringify(statements));
            
            const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
            console.log("dbTxResult", dbTxResult);

            if(body.queueId) {
                let _updateResult = {
                    transactionHash: updateResult.transactionHash
                }
                sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , result = '${JSON.stringify(_updateResult)}' where PK = 'QUEUE#UPDATE#${body.queueId}' and SK = '${member.SK}'`;
                let updateQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("updateQueueResult", updateQueueResult);
            }
            
            return {
                Success: true,
                Data: {
                    transactionHash: updateResult.transactionHash
                }
            }
        }
        else {
            console.log('Failed to update NFT in blockchain');
            return {
                Success: false,
                Message: "Failed to update NFT in blockchain"
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-update-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-update-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-update-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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