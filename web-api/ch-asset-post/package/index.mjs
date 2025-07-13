import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
// import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function getFileMIME(fileExtension) {
    console.log("fileExtension",fileExtension);
    
    var mimeType;
    switch (fileExtension.toLowerCase()) {

        case 'json':
            mimeType = 'application/json';
            break;

        case 'png':
            mimeType = 'image/png';
            break;

        case 'gif':
            mimeType = 'image/gif';
            break;
        
        case 'jpe':
        case 'jpg':
        case 'jpeg':
            mimeType = 'image/jpeg';
            break;
            
        case 'webp':
            mimeType = 'image/webp';
            break;
            
        case 'svg':
            mimeType = 'image/svg+xml';
            break;
            
        case 'unity3d':
            mimeType = 'application/vnd.unity';
            break;
            
        case 'mp3':
            mimeType = 'audio/mp3';
            break;
            
        case 'mp4':
            mimeType = 'video/mp4';
            break;

        case 'txt':
            mimeType = 'text/plain';
            break;

        case 'gltf':
            mimeType = 'model/gltf+json';
            break;

        case 'glb':
            mimeType = 'model/gltf-binary';
            break;

        case 'fbx':
            mimeType = 'application/fbx';
            break;

        case 'html':
            mimeType = 'text/html';
            break;
            
        default:
            throw Error('Unexpected file type');
    }
    
    return mimeType;
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
        
        if(!body.assetId){
            return {
                Success: false,
                Message: 'assetId is required',
            };
        }

        if(!body.nftFile){
            return {
                Success: false,
                Message: 'nftFile is required',
            };
        }

        if(!body.previewImageFile){
            return {
                Success: false,
                Message: 'previewImageFile is required',
            };
        }

        // if(!body.metadataFile){
        //     return {
        //         Success: false,
        //         Message: 'metadataFile is required',
        //     };
        // }

        if(!body.metadata){
            return {
                Success: false,
                Message: 'metadata is required',
            };
        }

        if(!body.name){
            return {
                Success: false,
                Message: 'name is required',
            };
        }

        if(!body.storeId){
            return {
                Success: false,
                Message: 'storeId is required',
            };
        }

        if(!body.category){
            return {
                Success: false,
                Message: 'category is required',
            };
        }

        if(!body.licenseId){
            return {
                Success: false,
                Message: 'licenseId is required',
            };
        }

        if(!body.nftType){
            return {
                Success: false,
                Message: 'nftType is required',
            };
        }

        if(!body.unit){
            return {
                Success: false,
                Message: 'cardano nft unit is required',
            };
        }

        if(!body.policyId){
            return {
                Success: false,
                Message: 'cardano policyId is required',
            };
        }

        if(!body.assetName){
            return {
                Success: false,
                Message: 'cardano assetName is required',
            };
        }

        let member;

        if(body.passcode == configs.find(x=>x.key == 'PASSCODE').value)
        {
            if(!body.memberId){
                return {
                    Success: false,
                    Message: 'memberId is required',
                };
            }
            let sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }
            member = memberResult.Items.map(unmarshall)[0];
        }
        else if (body.appPubKey) {
            
            // web3auth

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

            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}));
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
                Message: "Missing login info"
            }
        }
        
        console.log('inserting new asset');
        // 'token_id': '',
        // 'metadata_url': '${body.metadataFile}',
        let sql = `INSERT INTO "${tableName}" 
                    VALUE {
                        'PK': '${'ASSET#' + body.unit}',
                        'SK': '${'MEMBERWALLET#' + member.wallet_address}',
                        'type': 'ASSET',
                        'unit': '${body.unit}',
                        'policy_id': '${body.policyId}',
                        'asset_name': '${body.assetName}',
                        'asset_id': '${body.assetId}',
                        'owner_address': '${member.wallet_address}',
                        'owner_user_id': '${member.user_id}',
                        'owned_quantity': 1,
                        'name': '${body.name}',
                        'description': '${body.description ? body.description : ''}',
                        'max_supply': 1,
                        'asset_type': 'ERC721',
                        'liked_count': 0,
                        'royalties_percentage': '${body.royalty ? body.royalty : 0}',
                        'asset_url': '${body.hiddenFile ? body.hiddenFile : body.previewImageFile}',
                        'asset_thumbnail_url': '${body.previewImageFile}',
                        'nft_url': '${body.nftFile}',
                        'metadata': ? ,
                        'author_user_id': '${member.user_id}',
                        'author_address': '${member.wallet_address}',
                        'blockchain': 'CARDANO',
                        'tags': ? ,
                        'category_name': '${body.category}',
                        'sub_category_name': '${body.subCategory}',
                        'status': 'NOTFORSALE',
                        'media_type': '${body.fileExtension ? getFileMIME(body.fileExtension) : getFileMIME(body.previewImageFile.split(',').pop())}',
                        'store_id': '${body.storeId}',
                        'license_id': '${body.licenseId}',
                        'local_url': '${body.localURL}',`;
               
        if(body.encryptionKey) {
            sql += `'encryption_key': '${body.encryptionKey}',`;
        }

        if(body.chatDataHash) {
            sql += `'chat_data_hash': '${body.chatDataHash}',`;
        }

        if(body.chatDataLocalURL) {
            sql += `'chat_data_local_url': '${body.chatDataLocalURL}',`;
        }
                        
        sql += `'created_date': '${new Date().toISOString()}'
                    }`;
        console.log(sql);
        let statement = { Statement: sql, Parameters: [{S: JSON.stringify(body.metadata)}, {S: JSON.stringify(body.tags)}]};
                        
        console.log(statement);
        let dbResult = await dbClient.send(new ExecuteStatementCommand(statement));
        console.log("create asset dbResult", dbResult);
        
        const response = {
            Success: true
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-asset-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-asset-post - ' + random10DigitNumber,
            Message: `Error in ch-asset-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
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