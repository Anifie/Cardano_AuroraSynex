import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import md5 from 'md5';
import { ulid } from 'ulid';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
// const path = require('path');
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
        configs = configResult.Items.map(item => unmarshall(item));
        console.log("configs", configs);


        let token = headers['authorization'];
        console.log("token", token);

        let memberId = null;
        let member;

        if (!body.appPubKey && token) {
            try {
                const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key == 'JWT_SECRET').value);
                console.log("decoded", decoded);

                memberId = decoded.MemberId;

                if (Date.now() >= decoded.exp * 1000) {
                    return {
                        Success: false,
                        Message: "Token expired"
                    };
                }
            } catch (e) {
                console.log("error verify token", e);
                return {
                    Success: false,
                    Message: "Invalid token."
                };
            }

            let sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBER_ID#${memberId}' AND type = 'MEMBER' AND begins_with("PK", 'MEMBER#')`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if (memberResult.Items.length === 0) {
                console.log("member not found: " + memberId);
                return {
                    Success: false,
                    Message: "member not found: " + memberId
                };
            }
            member = memberResult.Items.map(item => unmarshall(item))[0];

            if (!member.role?.includes('ADMIN')) {
                return {
                    Success: false,
                    Message: "Unauthorized access"
                };
            }

            // if (!body.memberId) {
            //     return {
            //         Success: false,
            //         Message: "memberId is required"
            //     };
            // }

            // sql = `SELECT * FROM "${tableName}" WHERE PK = 'MEMBER#${body.memberId}' AND type = 'MEMBER'`;
            // memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // if (memberResult.Items.length === 0) {
            //     console.log("member not found: " + body.memberId);
            //     return {
            //         Success: false,
            //         Message: "member not found: " + body.memberId
            //     };
            // }

            // member = memberResult.Items.map(item => unmarshall(item))[0];


        } else if (body.appPubKey){
            // web3auth

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

            let sql = `SELECT * FROM "${tableName}" WHERE PK = 'MEMBER#${userId}' AND type = 'MEMBER'`;
            let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if (memberResult.Items.length === 0) {
                console.log("member not found: " + memberId);
                return {
                    Success: false,
                    Message: "member not found: " + memberId
                };
            }

            member = memberResult.Items.map(item => unmarshall(item))[0];

        } else {
            return {
                Success: false,
                Message: "Missing login info"
            };
        }


        // CAR, CHARACTER, MEMBERSHIP
        if(!body.category) {
            return {
                Success: false,
                Message: "category is required"
            }
        }

        // for CAR, is N-BOX , W-RV , CIVIC, TYPE R
        if(body.category == 'CAR' && !body.subCategory) {
            return {
                Success: false,
                Message: "subCategory is required for CAR"
            }
        }

        // artworkType : COMPONENT, FULL_TEMPLATE, FULL_USER 
        if(body.artworkType === undefined){
            return {
                Success: false, 
                Message: 'artworkType is required',
            };
        }

        if(body.status !== undefined){
            if(body.status != 'NEW' && body.status != 'LIVE')
            return {
                Success: false, 
                Message: 'Invalid status',
            };
        }

        // if(!body.twoDFileName) {
        //     return {
        //         Success: false,
        //         Message: 'twoDFileName is required'
        //     };
        // }

        // if(!body.threeDFileName) {
        //     return {
        //         Success: false,
        //         Message: 'threeDFileName is required'
        //     };
        // }

        // if(body.twoDBase64 === undefined){
        //     return {
        //         Success: false, 
        //         Message: 'twoDBase64 is required',
        //     };
        // }

        // if(body.threeDBase64 === undefined){
        //     return {
        //         Success: false, 
        //         Message: 'threeDBase64 is required',
        //     };
        // }

        if(body.artworkType === 'COMPONENT') {

            // if(member.role !== 'ADMIN') {
            //     return {
            //         Success: false,
            //         Message: "Only Admin can add component"
            //     };
            // }

            if(!body.componentNameEN) {
                return {
                    Success: false,
                    Message: "componentNameEN is required"
                }
            }
    
            if(!body.componentValueEN) {
                return {
                    Success: false,
                    Message: "componentValueEN is required"
                }
            }

            //if is non-admin user, add memberId as part of the component name
            if(body.appPubKey) {
                body.componentNameEN = `${member.user_id}#${body.componentNameEN}`;
            }

            // if(!body.componentNameJP) {
            //     return {
            //         Success: false,
            //         Message: "componentNameJP is required"
            //     }
            // }
    
            // if(!body.componentValueJP) {
            //     return {
            //         Success: false,
            //         Message: "componentValueJP is required"
            //     }
            // }
        }
        else if (body.artworkType === 'FULL_TEMPLATE' || body.artworkType === 'FULL_USER') {
            if((body.category === 'CAR') && body.components === undefined) {
                return {
                    Success: false, 
                    Message: 'components is required for CAR',
                };
            }

            if(body.artworkType === 'FULL_TEMPLATE' && member.role !== 'ADMIN') {
                return {
                    Success: false,
                    Message: "Only Admin can add full 3d as template"
                };
            }
        }
        
        

        // if(body.name === undefined){
        //     return {
        //         Success: false, 
        //         Message: 'name is required',
        //     };
        // }

        // if(body.description === undefined){
        //     return {
        //         Success: false, 
        //         Message: 'description is required',
        //     };
        // }

        // if(body.artworkMetadata === undefined){
        //     return {
        //         Success: false, 
        //         Message: 'artworkMetadata is required',
        //     };
        // }
        

        if(tableName == process.env.TABLE_NAME && body.appPubKey && body.artworkType == 'FULL_USER' && body.category == 'CAR') {
            let today = new Date().toISOString();
            if(today > process.env.CAR_END_DATE) {
                return {
                    Success: false, 
                    Message: '新しい車の追加は無効になっています',  //Add new car is disabled
                };
            }
        }

        if(body.artworkType == 'FULL_USER' && body.category == 'CAR' && body.subCategory) {
            let sql  = `select * from "${tableName}"."InvertedIndex" where SK = '${member.SK}' and type = 'ARTWORK' and artwork_type = '${body.artworkType}' and sub_category = '${body.subCategory}' and category = '${body.category}' and user_id = '${member.user_id}'`;
            let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(artworkResult.Items.length > 0 && !process.env.EXCEPTION_MEMBER_IDS.includes(member.user_id)) {
                console.log("Member already owned this type of car. メンバーはすでにこのタイプの車を所有しています");
                return {
                    Success: false,
                    Message: 'メンバーはすでにこのタイプの車を所有しています'   //Member already owned this type of car
                }
            }

            if(member.nft_member_b_token_id === undefined || member.nft_member_b_token_id === '') {
                console.log("You are not member of MetaGarage . あなたはMetaGarageのメンバーではありません");
                return {
                    Success: false,
                    Message: 'あなたはMetaGarageのメンバーではありません'   //You are not member of MetaGarage
                }
            }
        }

        const artworkId = ulid();

        let twoDUploadResult;
        if(body.twoDBase64 && body.twoDBase64 != '-1') {

            if(!body.twoDFileName) {
                return {
                    Success: false,
                    Message: "twoDFileName is required"
                };
            }

            twoDUploadResult = await fileUpload({
                assetId: artworkId,
                isBase64: true,
                fileData: body.twoDBase64.split(',').pop(),
                fileName: body.twoDFileName,
                fileExtension: body.twoDFileName.split('.').pop(),
                skipNFTStorage: true,
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                //isTest: tableName == process.env.process.env.TABLE_NAME_TEST
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
                assetId: artworkId + "_2",
                isBase64: true,
                fileData: body.twoDBase64_2.split(',').pop(),
                fileName: body.twoDFileName_2,
                fileExtension: body.twoDFileName_2.split('.').pop(),
                skipNFTStorage: true,
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                // isTest: tableName == process.env.process.env.TABLE_NAME_TEST
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
                assetId: artworkId + "_3",
                isBase64: true,
                fileData: body.twoDBase64_3.split(',').pop(),
                fileName: body.twoDFileName_3,
                fileExtension: body.twoDFileName_3.split('.').pop(),
                skipNFTStorage: true,
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                // isTest: tableName == process.env.process.env.TABLE_NAME_TEST
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
                assetId: artworkId,
                isBase64: true,
                fileData: body.threeDBase64.split(',').pop(),
                fileName: body.threeDFileName,
                fileExtension: body.threeDFileName.split('.').pop(),
                skipNFTStorage: true,
                S3URL: configs.find(x=>x.key == 'S3_URL').value,
                S3BucketName: configs.find(x=>x.key == 'S3_BUCKET').value,
                SNSTopic: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value,
                // isTest: tableName == process.env.process.env.TABLE_NAME_TEST
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

        // let videoUploadResult;
        let videoPresignedUploadURL;
        let videoMIMEType;
        let videoKey;
        let videoExtension;
        if(body.videoFileName) {

            //get s3 presigned URL
            
            const bucketVideo = configs.find(x=>x.key == 'S3_BUCKET').value;    //tableName === process.env.process.env.TABLE_NAME ? process.env.S3_BUCKET_VIDEO : process.env.S3_BUCKET_VIDEO_TEST;  
            const expireSeconds = 60 * 5;        
            videoKey = artworkId + '_video';
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

        
        // if user have ARTIST role, he can illuminate lantern on behave of another user
        console.log('inserting new artwork');
        let newArtworkSqlParamters = [];
        //'message': '${body.message}',
        //'two_d_base64': '${body.twoDBase64 ? body.twoDBase64 : ''}',
        //'three_d_base64': '${body.threeDBase64 ? body.threeDBase64 : ''}',
        let newArtworkSql = `INSERT INTO "${tableName}" 
                            VALUE {
                                    'PK': '${'ARTWORK#' + artworkId}',
                                    'SK': '${member.SK}',
                                    'type': 'ARTWORK',
                                    'artwork_id': '${artworkId}',
                                    'artwork_type': '${body.artworkType}',
                                    'category': '${body.category}',
                                    'sub_category': '${body.subCategory ? body.subCategory : ''}',
                                    'two_d_file_name': '${body.twoDFileName ? body.twoDFileName : ''}',
                                    'two_d_file_name_2': '${body.twoDFileName_2 ? body.twoDFileName_2 : ''}',
                                    'two_d_file_name_3': '${body.twoDFileName_3 ? body.twoDFileName_3 : ''}',
                                    'three_d_file_name': '${body.threeDFileName ? body.threeDFileName : ''}',
                                    'video_file_name': '${body.videoFileName ? body.videoFileName : ''}',
                                    'is_available': true,`;


        if(!member.role?.includes('ADMIN')) {
            if(!member.user_id) {
                return {
                    Success: false,
                    Message: 'Missing memberId'
                }
            }
            newArtworkSql += ` 'user_id': '${member.user_id}',`;
        }
                                    
        if(body.componentNameEN && body.componentValueEN) {
            newArtworkSql += ` 'name_en': '${body.componentNameEN}', 'value_en': '${body.componentValueEN}',`;

            let sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_EN#${body.componentNameEN}'`;
            let result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("sql", sql);
            console.log("result", result);
        
            if(result.Items.length === 0) {
                console.log(`insert ENUM for component ${body.componentNameEN} , value ${body.componentValueEN}`);
                txStatements.push({ 
                    Statement: `INSERT INTO "${tableName}" VALUE { 'PK': ? , 'SK': ?, 'type': ?, 'enum_name': ?, 'enum_values': ?, 'enum_description': ?, 'created_date': ?}`,
                    Parameters: [{ S: 'ENUM'}, { S: `COMPONENT_EN#${body.componentNameEN}` }, { S: 'ENUM'}, { S: `${body.componentNameEN}` }, 
                                    { S: `${body.componentValueEN}` }, 
                                    { S: `${artworkId}` }, 
                                    { S: new Date().toISOString() }
                                ]});                
            }
            else {
                console.log(`update ENUM for component ${body.componentNameEN} , value ${body.componentValueEN}`);
                let componentEnum = result.Items.map(unmarshall)[0];
                txStatements.push({ 
                    Statement: `UPDATE "${tableName}" SET modified_date = ?, enum_values = ?, enum_description = ? WHERE PK = ? and SK = ?`,
                    Parameters: [
                                    { S: new Date().toISOString() },
                                    { S: componentEnum.enum_values + ',' + body.componentValueEN },
                                    { S: componentEnum.enum_description + ',' + artworkId },
                                    { S: componentEnum.PK },
                                    { S: componentEnum.SK }
                                ]});  
            }
        }

        if(body.componentNameJP && body.componentValueJP) {
            newArtworkSql += ` 'name_jp': '${body.componentNameJP}', 'value_jp': '${body.componentValueJP}',`;

            let result = await db.executeStatement({
                Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_JP#${body.componentNameJP}'`
            }).promise();
        
            if(result.Items.length === 0) {
                console.log(`insert ENUM for component ${body.componentNameJP} , value ${body.componentValueJP}`);
                txStatements.push({ 
                    Statement: `INSERT INTO "${tableName}" VALUE { 'PK': ? , 'SK': ?, 'type': ?, 'enum_name': ?, 'enum_values': ?, 'enum_description': ?, 'created_date': ?}`,
                    Parameters: [{ S: 'ENUM'}, { S: `COMPONENT_JP#${body.componentNameJP}` }, { S: 'ENUM'}, { S: `${body.componentNameJP}` }, 
                                    { S: `${body.componentValueJP}` }, 
                                    { S: `${artworkId}` }, 
                                    { S: new Date().toISOString() }
                                ]});                
            }
            else {
                console.log(`update ENUM for component ${body.componentNameJP} , value ${body.componentValueJP}`);
                let componentEnum = result.Items.map(unmarshall)[0];
                txStatements.push({ 
                    Statement: `UPDATE "${tableName}" SET modified_date = ?, enum_values = ?, enum_description = ? WHERE PK = ? and SK = ?`,
                    Parameters: [
                                    { S: new Date().toISOString() },
                                    { S: componentEnum.enum_values + ',' + body.componentValueJP },
                                    { S: componentEnum.enum_description + ',' + artworkId },
                                    { S: componentEnum.PK },
                                    { S: componentEnum.SK }
                                ]});  
            }
        }

        if(body.components && body.components.length > 0) {
            newArtworkSql += ` 'components': '${JSON.stringify(body.components)}', `;
        }

        if(twoDUploadResult) {
            newArtworkSql += ` 'two_d_url': '${twoDUploadResult.localURL}', 'two_d_mime': '${twoDUploadResult.mimeType}', `;
        }
        else if(body.twoDBase64 === '-1') {
            newArtworkSql += ` 'two_d_url' : '', 'two_d_mime' : '', `;
        }

        if(twoDUploadResult2) {
            newArtworkSql += ` 'two_d_url_2': '${twoDUploadResult2.localURL}', 'two_d_mime_2': '${twoDUploadResult2.mimeType}', `;
        }
        else if(body.twoDBase64_2 === '-1') {
            newArtworkSql += ` 'two_d_url_2' : '', 'two_d_mime_2' : '', `;
        }
        
        if(twoDUploadResult3) {
            newArtworkSql += ` 'two_d_url_3': '${twoDUploadResult3.localURL}', 'two_d_mime_3': '${twoDUploadResult3.mimeType}', `;
        }
        else if(body.twoDBase64_3 === '-1') {
            newArtworkSql += ` 'two_d_url_3' : '', 'two_d_mime_3' : '', `;
        }

        if(threeDUploadResult) {
            newArtworkSql += ` 'three_d_url': '${threeDUploadResult.localURL}', 'three_d_mime': '${threeDUploadResult.mimeType}', `;
        }
        else if(body.threeDBase64 === '-1') {
            newArtworkSql += ` 'three_d_url' : '', 'three_d_mime' : '', `;
        }
        
        if(videoPresignedUploadURL) {
            let s3URL = configs.find(x=>x.key == 'S3_URL').value;    //tableName === process.env.TABLE_NAME ? process.env.S3_URL : process.env.S3_URL_TEST;
            newArtworkSql += ` 'video_url': '${s3URL + "/videos/" + videoKey + videoExtension}', 
                                'video_mime': '${videoMIMEType}', `;
        }
        else if(body.videoFileName === '-1') {
            newArtworkSql += ` 'video_url' : '', 'video_mime' : '', `;
        }

        if(body.name) {
            newArtworkSql += ` 'name': ? , `;
            newArtworkSqlParamters.push({S: body.name })
        }

        if(body.description) {
            newArtworkSql += ` 'description': ? , `;
            newArtworkSqlParamters.push({S: body.description })
        }

        if(body.prompt) {
            newArtworkSql += ` 'prompt': ? , `;
            newArtworkSqlParamters.push({S: body.prompt })
        }

        if(body.metadata) {
            newArtworkSql += ` 'metadata': ? , `;
            newArtworkSqlParamters.push({S: JSON.stringify(body.metadata) })
        }

        if(body.components) {
            newArtworkSql += ` 'components': ? , `;
            newArtworkSqlParamters.push({S: JSON.stringify(body.components) })
        }

        if(body.status) {
            newArtworkSql += ` 'status': '${body.status}', `;
        }
        else {
            newArtworkSql += ` 'status': 'NEW', `;
        }

        newArtworkSql += `  'created_date': '${new Date().toISOString()}',
                            'created_by': '${member.user_id}'
                        }`;
        console.log(newArtworkSql);
        txStatements.push({ "Statement": newArtworkSql, "Parameters": newArtworkSqlParamters });

        
        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("add artwork dbResult", dbTxResult);

        const response = {
            Success: true,
            Data: {
                artworkId: artworkId,
                S3PresignedVideoURL: videoPresignedUploadURL
            }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-artwork-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-artwork-post- ' + random10DigitNumber,
            Message: `Error in ch-artwork-post ${e.message}\n\nStack trace:\n${e.stack}`,
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