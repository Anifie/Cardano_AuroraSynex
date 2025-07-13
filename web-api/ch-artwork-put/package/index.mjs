import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });
const lambdaClient = new LambdaClient({
    region: process.env.AWS_REGION,
    maxAttempts: 1, // equivalent to maxRetries: 0 in SDK v2
    requestHandler: {
        requestTimeout: 1 * 60 * 1000 // 1 minutes in milliseconds
    }
});


const fileUpload = async (params) => {
    console.log("fileUpload", params);

    let lambdaParams = {
        FunctionName: 'ch-file-upload-post',
        InvocationType: 'RequestResponse', 
        LogType: 'Tail',
        Payload: {
            body: JSON.stringify({
                assetId: params.assetId, 
                fileData: params.fileData, 
                fileName: params.fileName, 
                fileExtension: params.fileExtension, 
                params: params.params, 
                isBase64: params.isBase64, 
                skipNFTStorage: params.skipNFTStorage,
                S3URL: params.S3URL,
                S3BucketName: params.S3BucketName,
                SNSTopic: params.SNSTopic
            })
        }
    };
    lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
    console.log("lambdaParams", lambdaParams);            
    
    //const lambdaResult = await lambda.invoke(lambdaParams).promise();        
    const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
    console.log("lambdaResult", lambdaResult);

    const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
    console.log("payload", payload);
    
    if (payload.errorMessage) {
        console.error("lambda error message:", JSON.stringify(payload.errorMessage));
        throw new Error('Lambda error: ' + JSON.stringify(payload.errorMessage));
    }
    
    // const uploaded = JSON.parse(lambdaResult.Payload).Success;    
    // if(lambdaResult.Payload.errorMessage) {
    //     console.log("file upload lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
    //     throw new Error('fileupload Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
    // }            
    // const uploadResult = JSON.parse(lambdaResult.Payload);    
    // console.log("uploadResult", uploadResult);

    // if(uploadResult) {
    //     return uploadResult.body;
    // }

    return payload.body;

}

function GetFileMIME(fileExtension) {
    console.log("fileExtension",fileExtension);
    
    var mimeType;
    switch (fileExtension.toLowerCase()) {
        
        case '.png':
            mimeType = 'image/png';
            break;

        case '.gif':
            mimeType = 'image/gif';
            break;
        
        case '.jpe':
        case '.jpg':
        case '.jpeg':
            mimeType = 'image/jpeg';
            break;
            
        case '.webp':
            mimeType = 'image/webp';
            break;
            
        case '.svg':
            mimeType = 'image/svg+xml';
            break;
            
        case '.unity3d':
            mimeType = 'application/vnd.unity';
            break;
            
        case '.mp3':
            mimeType = 'audio/mp3';
            break;
            
        case '.mp4':
            mimeType = 'video/mp4';
            break;
            
        default:
            throw Error('Expected file type');
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
        configs = configResult.Items.map(unmarshall);
        console.log("configs", configs);

        var token = headers['authorization'];
        console.log("token", token);
        
        let memberId = null;
        let member;

        if(!body.appPubKey && token) {
            //verify token
            try{
                const decoded = jwt.verify(token.split(' ')[1], configs.find(x => x.key == 'JWT_SECRET').value);
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
            console.log('Missing login info');
            const response = {
                    Success: false,
                    Message: "Missing login info"
                };
            return response;
        }

        if(!body.artworkId) {
            return {
                Success: false,
                Message: "artworkId is required"
            }
        }
        
        if(body.status && body.status !== 'NEW' && body.status !== 'LIVE') {
            return {
                Success: false,
                Message: "Invalid status. Must be either NEW or LIVE"
            }
        }

        let sql = `select * from "${tableName}" where PK = '${'ARTWORK#' + body.artworkId}' and type = 'ARTWORK'`;
        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(artworkResult.Items.length == 0) {
            console.log("artwork not found: " + body.artworkId);
            const response = {
                Success: false,
                Message: "artwork not found: " + body.artworkId
            };
            return response;
        }
        let artwork = artworkResult.Items.map(unmarshall)[0];

        // if(artwork.status === 'LIVE') {
        //     let today = new Date().toISOString();
        //     if(tableName == process.env.TABLE_NAME) {
        //         console.log(today, process.env.EXHIBITION_DATE_PROD);
        //         if(today > process.env.EXHIBITION_DATE_PROD) {
        //             console.log("Exhibition had started, artworks cannot be changed", today, process.env.EXHIBITION_DATE_PROD);
                    
        //             return {
        //                 Success: false,
        //                 Message: '展覧会が始まったので作品の変更はできません'   // Exhibition had started, artworks cannot be changed
        //             }
        //         }
        //     }
        //     else if(tableName == process.env.TABLE_NAME_TEST) {
        //         console.log(today, process.env.EXHIBITION_DATE_TEST);
        //         if(today > process.env.EXHIBITION_DATE_TEST) {
        //             console.log("Exhibition had started, artworks cannot be changed", today, process.env.EXHIBITION_DATE_TEST);
        //             return {
        //                 Success: false,
        //                 Message: '展覧会が始まったので作品の変更はできません'   // Exhibition had started, artworks cannot be changed
        //             }
        //         }
        //     }
        // }
        
        if(artwork.ar_tx_id) {
            return {
                Success: false,
                Message: "Cannot edit as this artwork already been minted as NFT"
            }
        }

        if(!member.role?.includes('ADMIN')) {
            if(member.user_id != artwork.user_id) {
                return {
                    Success: false,
                    Message: 'This artwork does not belong to this user'
                }
            }    
        }

        // if(tableName == process.env.TABLE_NAME && body.appPubKey && artwork.category == 'CAR') {
        //     let today = new Date().toISOString();
        //     if(today > process.env.CAR_END_DATE) {
        //         return {
        //             Success: false, 
        //             Message: '車の更新が無効になっています',  //Update car is disabled
        //         };
        //     }
        // }

        let twoDUploadResult;
        if(body.twoDBase64 && body.twoDBase64 != '-1') {

            if(!body.twoDFileName) {
                return {
                    Success: false,
                    Message: "twoDFileName is required"
                };
            }

            twoDUploadResult = await fileUpload({
                assetId: body.artworkId,
                isBase64: true,
                fileData: body.twoDBase64.split(',').pop(),
                fileName: body.twoDFileName,
                fileExtension: body.twoDFileName.split('.').pop(),
                skipNFTStorage: true,
                // isTest: tableName == process.env.TABLE_NAME_TEST
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,

                // params: {
                //     name: body.name,
                //     description: body.description,
                //     attributes: body.metadata
                //     // item_width: previewWidth,
                //     // item_height: previewHeight,
                //     // creator_fee_basis_points: data.royalty !== undefined ? data.royalty * 100 : 0,
                //     //creator_fee_recipient: "",
                //     // attributes: [
                //     //                 {"trait_type": "HiddenFile", "value": "ipfs://" + cidHiddenFile.metadata}, 
                //     //                 {"trait_type": "HiddenFileOriginalName", "value": assetId + "." + data.mainfile[0].name.split('.').pop()},
                //     //                 {"trait_type": "HiddenFileSizeInKB", "value": mainFileSizeKB}, 
                //     //                 {"trait_type": "License", "value": getLicenseURL(data.license)}
                //     //             ]
                // }
            })

            twoDUploadResult = JSON.parse(twoDUploadResult);
            console.log("twoDUploadResult", twoDUploadResult)
        }

        let twoDUploadResult2;
        if(body.twoDBase64_2 && body.twoDBase64_2 != '-1') {

            if(!body.twoDFileName_2) {
                return {
                    Success: false,
                    Message: "twoDFileName_2 is required"
                };
            }

            twoDUploadResult2 = await fileUpload({
                assetId: body.artworkId + "_2",
                isBase64: true,
                fileData: body.twoDBase64_2.split(',').pop(),
                fileName: body.twoDFileName_2,
                fileExtension: body.twoDFileName_2.split('.').pop(),
                skipNFTStorage: true,
                // isTest: tableName == process.env.TABLE_NAME_TEST
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,

                // params: {
                //     name: body.name,
                //     description: body.description,
                //     attributes: body.metadata
                //     // item_width: previewWidth,
                //     // item_height: previewHeight,
                //     // creator_fee_basis_points: data.royalty !== undefined ? data.royalty * 100 : 0,
                //     //creator_fee_recipient: "",
                //     // attributes: [
                //     //                 {"trait_type": "HiddenFile", "value": "ipfs://" + cidHiddenFile.metadata}, 
                //     //                 {"trait_type": "HiddenFileOriginalName", "value": assetId + "." + data.mainfile[0].name.split('.').pop()},
                //     //                 {"trait_type": "HiddenFileSizeInKB", "value": mainFileSizeKB}, 
                //     //                 {"trait_type": "License", "value": getLicenseURL(data.license)}
                //     //             ]
                // }
            })

            twoDUploadResult2 = JSON.parse(twoDUploadResult2);
            console.log("twoDUploadResult2", twoDUploadResult2)
        }

        let twoDUploadResult3;
        if(body.twoDBase64_3 && body.twoDBase64_3 != '-1') {

            if(!body.twoDFileName_3) {
                return {
                    Success: false,
                    Message: "twoDFileName_3 is required"
                };
            }

            twoDUploadResult3 = await fileUpload({
                assetId: body.artworkId + "_3",
                isBase64: true,
                fileData: body.twoDBase64_3.split(',').pop(),
                fileName: body.twoDFileName_3,
                fileExtension: body.twoDFileName_3.split('.').pop(),
                skipNFTStorage: true,
                // isTest: tableName == process.env.TABLE_NAME_TEST
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                // params: {
                //     name: body.name,
                //     description: body.description,
                //     attributes: body.metadata
                //     // item_width: previewWidth,
                //     // item_height: previewHeight,
                //     // creator_fee_basis_points: data.royalty !== undefined ? data.royalty * 100 : 0,
                //     //creator_fee_recipient: "",
                //     // attributes: [
                //     //                 {"trait_type": "HiddenFile", "value": "ipfs://" + cidHiddenFile.metadata}, 
                //     //                 {"trait_type": "HiddenFileOriginalName", "value": assetId + "." + data.mainfile[0].name.split('.').pop()},
                //     //                 {"trait_type": "HiddenFileSizeInKB", "value": mainFileSizeKB}, 
                //     //                 {"trait_type": "License", "value": getLicenseURL(data.license)}
                //     //             ]
                // }
            })

            twoDUploadResult3 = JSON.parse(twoDUploadResult3);
            console.log("twoDUploadResult3", twoDUploadResult3)
        }

        let threeDUploadResult;
        if(body.threeDBase64 && body.threeDBase64 != '-1') {

            if(!body.threeDFileName) {
                return {
                    Success: false,
                    Message: "threeDFileName is required"
                };
            }

            threeDUploadResult = await fileUpload({
                assetId: body.artworkId,
                isBase64: true,
                fileData: body.threeDBase64.split(',').pop(),
                fileName: body.threeDFileName,
                fileExtension: body.threeDFileName.split('.').pop(),
                skipNFTStorage: true,
                // isTest: tableName == process.env.TABLE_NAME_TEST
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                // params: {
                //     name: body.name,
                //     description: body.description,
                //     attributes: body.metadata
                //     // item_width: previewWidth,
                //     // item_height: previewHeight,
                //     // creator_fee_basis_points: data.royalty !== undefined ? data.royalty * 100 : 0,
                //     //creator_fee_recipient: "",
                //     // attributes: [
                //     //                 {"trait_type": "HiddenFile", "value": "ipfs://" + cidHiddenFile.metadata}, 
                //     //                 {"trait_type": "HiddenFileOriginalName", "value": assetId + "." + data.mainfile[0].name.split('.').pop()},
                //     //                 {"trait_type": "HiddenFileSizeInKB", "value": mainFileSizeKB}, 
                //     //                 {"trait_type": "License", "value": getLicenseURL(data.license)}
                //     //             ]
                // }
            });

            threeDUploadResult = JSON.parse(threeDUploadResult);

            console.log("threeDUploadResult", threeDUploadResult)
        }

        let videoPresignedUploadURL;
        let videoMIMEType;
        let videoKey;
        let videoExtension;
        if(body.videoFileName) {

            //get s3 presigned URL
            const bucketVideo = configs.find(x=>x.key == 'S3_BUCKET').value;    //tableName === process.env.TABLE_NAME ? process.env.S3_BUCKET_VIDEO : process.env.S3_BUCKET_VIDEO_TEST;  
            const expireSeconds = 60 * 5;        
            videoKey = body.artworkId + '_video';
            videoExtension = path.extname(body.videoFileName);
            console.log("videoExtension",videoExtension);
            videoMIMEType = GetFileMIME(videoExtension);
            
            videoPresignedUploadURL = s3.getSignedUrl("putObject", {
                                                                        Bucket: bucketVideo,
                                                                        Key: videoKey + videoExtension,
                                                                        Expires: expireSeconds,
                                                                        ContentType: videoMIMEType,
                                                                        ACL: 'public-read'
                                                                    });
            console.log("videoPresignedUploadURL", videoPresignedUploadURL);
        }

        

        let txStatements = [];
        
        console.log('updating artwork');
        
        let updateSql = `UPDATE "${tableName}" 
                            SET modified_date = '${new Date().toISOString()}', `;

        if(body.componentNameEN !== artwork.name_en && body.componentValueEN !== artwork.value_en) {
            updateSql += ` name_en = '${body.componentNameEN}', value_en = '${body.componentValueEN}', `;

            let result = await dbClient.send(new ExecuteStatementCommand({
                Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_EN#${body.componentNameEN}'`
            }))
        
            if(result.Items.length === 0) {
                console.log(`insert ENUM for component ${body.componentNameEN} , value ${body.componentValueEN}`);
                txStatements.push({ 
                    Statement: `INSERT INTO "${tableName}" VALUE { 'PK': ? , 'SK': ?, 'type': ?, 'enum_name': ?, 'enum_values': ?, 'enum_description': ?, 'created_date': ?}`,
                    Parameters: [{ S: 'ENUM'}, { S: `COMPONENT_EN#${body.componentNameEN}` }, { S: 'ENUM'}, { S: `${body.componentNameEN}` }, 
                                    { S: `${body.componentValueEN}` }, 
                                    { S: `${body.artworkId}` }, 
                                    { S: new Date().toISOString() }
                                ]});                
            }
            else {
                console.log(`update ENUM for component ${body.componentNameEN} , value ${body.componentValueEN}`);
                let componentEnum = result.Items.map(unmarshall)[0];
                let enumValuesArr = componentEnum.enum_values.split(',');
                let enumDescsArr = componentEnum.enum_description.split(',');
                let enumValuesIndex = enumValuesArr.indexOf(artwork.value_en);
                let enumDescIndex = enumDescsArr.indexOf(body.artworkId);
                if(enumValuesIndex >= 0) {
                    let enumValuesNew = enumValuesArr.splice(enumValuesIndex, 1).push(body.componentValueEN).join(); // remove element by value, and insert new value
                    let enumDescsNew = enumDescsArr.splice(enumDescIndex, 1).push(body.artworkId).join(); // remove element by value, and insert new value
                    txStatements.push({ 
                        Statement: `UPDATE "${tableName}" SET modified_date = ?, enum_values = ?, enum_description = ? WHERE PK = ? and SK = ?`,
                        Parameters: [
                                        { S: new Date().toISOString() },
                                        { S: enumValuesNew },
                                        { S: enumDescsNew },
                                        { S: componentEnum.PK },
                                        { S: componentEnum.SK }
                                    ]});  
                }
            }
        }

        if(body.componentNameJP !== artwork.name_jp && body.componentValueJP !== artwork.value_jp) {
            updateSql += ` name_jp = '${body.componentNameJP}', value_jp = '${body.componentValueJP}', `;

            let result = await dbClient.send(new ExecuteStatementCommand({
                Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_JP#${body.componentNameJP}'`
            }))
        
            if(result.Items.length === 0) {
                console.log(`insert ENUM for component ${body.componentNameJP} , value ${body.componentValueJP}`);
                txStatements.push({ 
                    Statement: `INSERT INTO "${tableName}" VALUE { 'PK': ? , 'SK': ?, 'type': ?, 'enum_name': ?, 'enum_values': ?, 'enum_description': ?, 'created_date': ?}`,
                    Parameters: [{ S: 'ENUM'}, { S: `COMPONENT_JP#${body.componentNameJP}` }, { S: 'ENUM'}, { S: `${body.componentNameJP}` }, 
                                    { S: `${body.componentValueJP}` }, 
                                    { S: `${body.artworkId}` }, 
                                    { S: new Date().toISOString() }
                                ]});                
            }
            else {
                console.log(`update ENUM for component ${body.componentNameJP} , value ${body.componentValueJP}`);
                let componentEnum = result.Items.map(unmarshall)[0];
                let enumValuesArr = componentEnum.enum_values.split(',');
                let enumDescsArr = componentEnum.enum_description.split(',');
                let enumValuesIndex = enumValuesArr.indexOf(artwork.value_jp);
                let enumDescIndex = enumDescsArr.indexOf(body.artworkId);
                if(enumValuesIndex >= 0) {
                    let enumValuesNew = enumValuesArr.splice(enumValuesIndex, 1).push(body.componentValueJP).join(); // remove element by value, and insert new value
                    let enumDescsNew = enumDescsArr.splice(enumDescIndex, 1).push(body.artworkId).join(); // remove element by value, and insert new value
                    txStatements.push({ 
                        Statement: `UPDATE "${tableName}" SET modified_date = ?, enum_values = ?, enum_description = ? WHERE PK = ? and SK = ?`,
                        Parameters: [
                                        { S: new Date().toISOString() },
                                        { S: enumValuesNew },
                                        { S: enumDescsNew },
                                        { S: componentEnum.PK },
                                        { S: componentEnum.SK }
                                    ]});  
                }
            }
        }

        if(body.components && body.components.length > 0) {
            updateSql += ` components = '${JSON.stringify(body.components, null, 0)}', `;
        }

        if(twoDUploadResult) {
            updateSql += ` two_d_url = '${twoDUploadResult.localURL}', two_d_mime = '${twoDUploadResult.mimeType}', two_d_file_name = '${body.twoDFileName}', `;
        }
        else if(body.twoDBase64 === '-1') {
            updateSql += ` two_d_url = '', two_d_mime = '', two_d_file_name = '', `;
        }

        if(twoDUploadResult2) {
            updateSql += ` two_d_url_2 = '${twoDUploadResult2.localURL}', two_d_mime_2 = '${twoDUploadResult2.mimeType}', two_d_file_name_2 = '${body.twoDFileName_2}', `;
        }
        else if(body.twoDBase64_2 === '-1') {
            updateSql += ` two_d_url_2 = '', two_d_mime_2 = '', two_d_file_name_2 = '', `;
        }
        
        if(twoDUploadResult3) {
            updateSql += ` two_d_url_3 = '${twoDUploadResult3.localURL}', two_d_mime_3 = '${twoDUploadResult3.mimeType}', two_d_file_name_3 = '${body.twoDFileName_3}', `;
        }
        else if(body.twoDBase64_3 === '-1') {
            updateSql += ` two_d_url_3 = '', two_d_mime_3 = '', two_d_file_name_3 = '', `;
        }

        if(threeDUploadResult) {
            updateSql += ` three_d_url = '${threeDUploadResult.localURL}', three_d_mime = '${threeDUploadResult.mimeType}', three_d_file_name = '${body.threeDFileName}', `;
        }
        else if(body.threeDBase64 === '-1') {
            updateSql += ` three_d_url = '', three_d_mime = '', three_d_file_name = '', `;
        }

        if(videoPresignedUploadURL) {
            let s3URL = configs.find(x=>x.key == 'S3_URL').value;   //tableName === process.env.TABLE_NAME ? process.env.S3_URL : process.env.S3_URL_TEST;
            updateSql += ` video_url = '${s3URL + "/videos/" + videoKey + videoExtension}', video_mime = '${videoMIMEType}', video_file_name = '${body.videoFileName}', `;
        }
        else if(body.videoFileName === '-1') {
            updateSql += ` video_url = '', video_mime = '', video_file_name = '', `;
        }

        if(body.metadata) {
            updateSql += ` metadata = '${JSON.stringify(body.metadata, null, 0)}', `;
        }

        if(body.components) {
            updateSql += ` components = '${JSON.stringify(body.components, null, 0)}', `;
        }

        if(body.status) {
            updateSql += ` status = '${body.status}', `;
        }

        if(body.name) {
            updateSql += ` name = '${body.name}', `;
        }

        if(body.description) {
            updateSql += ` description = '${body.description}', `;
        }

        if(body.category) {
            updateSql += ` category = '${body.category}', `;
        }

        if(body.subCategory) {
            updateSql += ` sub_category = '${body.subCategory}', `;
        }

        //reset liked_count to 0
        if(artwork.status == 'LIVE' && artwork.artwork_type == 'FULL_USER' && artwork.liked_count !== undefined && artwork.liked_count !== 0) {
            updateSql += ` liked_count = 0 , `;
        }

        if(updateSql.substring(updateSql.length - 2) === ', ')
            updateSql = updateSql.slice(0, -2);

        updateSql += `  where PK = '${artwork.PK}' and SK = '${artwork.SK}' `;
        console.log(updateSql);
        txStatements.push({ "Statement": updateSql});

        //delete all favourites for this artwork
        if(artwork.status == 'LIVE') {
            if(artwork.artwork_type == 'FULL_USER' && artwork.liked_count !== undefined && artwork.liked_count !== 0) {
                sql = `select * from "${tableName}" where PK = 'ARTWORKFAVOURITE#${artwork.artwork_id}' and type = 'ARTWORKFAVOURITE' `;
                let favResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                let favs = favResult.Items.map(unmarshall);
                for (let i = 0; i < favs.length; i++) {
                    const fav = favs[i];
                    sql = `DELETE FROM "${tableName}" WHERE PK = '${fav.PK}' and SK = '${fav.SK}'`;
                    txStatements.push({ "Statement": sql});
                }
            }
    
            //delete all comments for this artwork
            if(artwork.artwork_type == 'FULL_USER') {
                sql = `select * from "${tableName}" where PK = 'ARTWORK#${artwork.artwork_id}' and type = 'COMMENT' `;
                let commentResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                let comments = commentResult.Items.map(unmarshall);
                for (let i = 0; i < comments.length; i++) {
                    const comment = comments[i];
                    sql = `DELETE FROM "${tableName}" WHERE PK = '${comment.PK}' and SK = '${comment.SK}'`;
                    txStatements.push({ "Statement": sql});
                }
            }
        }
        
        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("update artwork dbResult", dbTxResult);

        return {
            Success: true,
            Data: {
                S3PresignedVideoURL: videoPresignedUploadURL
            }
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-artwork-put ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-artwork-put - ' + random10DigitNumber,
            Message: `Error in ch-artwork-put: ${e.message}\n\nStack trace:\n${e.stack}`,
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