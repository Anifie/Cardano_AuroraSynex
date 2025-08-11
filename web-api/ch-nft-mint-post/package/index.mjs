import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { ulid } from 'ulid';
import https from 'https';
// import { getNextSequence } from 'sequence-number';

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

export async function getNextSequence(counterId, sequenceName) {
  const result = await dbClient.send(new UpdateCommand({
    TableName: tableName,
    Key: {
      PK: counterId,    // Partition key
      SK: sequenceName  // Sort key
    },
    UpdateExpression: "SET CurrentValue = if_not_exists(CurrentValue, :start) + :inc",
    ExpressionAttributeValues: {
      ":start": 0,
      ":inc": 1
    },
    ReturnValues: "UPDATED_NEW"
  }));

  return result.Attributes.CurrentValue;
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

const nftPost = async (params, origin) => {
    console.log("nftPost", params);
    let response = await axios.post(configs.find(x=>x.key == 'API_URL').value + '/asset',
                        JSON.stringify(params),
                        {
                            headers: {
                                'Content-Type': 'application/json',
                                'origin': origin
                            }
                        }
                    );
    console.log('nftPost jsonResult', response.data);
    return response.data;
}

const web3Mint = async (params) => {
    const lambdaParams = {
        FunctionName: `ch-web3`,
        InvocationType: 'RequestResponse',
        LogType: 'Tail',
        Payload: params
    };

    try {
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        
        if (payload.errorMessage) {
            console.error("web3 mint lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('web3 mint Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        console.log("mint result", payload);
        return payload;
    } catch (error) {
        console.error("web3 mint Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
    }
};


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
}

const checkOwnerPost = async (params, origin) => {
    console.log("checkOwnerPost", params);
    let response = await axios.post(configs.find(x=>x.key == 'API_URL').value + '/nft/owner',
                        JSON.stringify(params),
                        {
                            headers: {
                                'Content-Type': 'application/json',
                                'origin': origin
                            }
                        }
                    );
    console.log('checkOwnerPost jsonResult', response.data);
    return response.data;
}


const folderUpload = async (params) => {
    console.log("folderUpload", params);

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

const folderUpload3 = async (params) => {
    console.log("folderUpload3", params);
    
    let lambdaParams = {
        FunctionName: 'ch-nft-folder-upload-post3',
        InvocationType: 'RequestResponse', 
        LogType: 'Tail',
        Payload: {
            artworkIdV1: params.artworkIdV1,
            artworkIdV2: params.artworkIdV2,
            artworkIdV3: params.artworkIdV3,
            isTest: params.isTest
        }
    };            
    try {
        lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
        const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
        const payload = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
        
        if (payload.errorMessage) {
            console.error("Upload folder3 lambda error message:", JSON.stringify(payload.errorMessage));
            throw new Error('Upload folder3 Lambda error: ' + JSON.stringify(payload.errorMessage));
        }

        console.log("upload folder3 result", payload);
        return payload;
    } catch (error) {
        console.error("Upload folder3 Lambda invocation error:", error);
        throw new Error('Lambda invocation failed: ' + error.message);
    }
}


const DEV_TEXTURE_MAP = {
    nbox: {
      "01J6725YGG6T4TVKJ1G4ZP8Y1T": "No.1",
      "01J67256RGFEN8FEDEYV4Q15FC": "No.2",
      "01J80QY0DG9JDNFWH0T2X6985Y": "No.3",
      "01J80QYMQKKPKBD71120J6Q2B2": "No.4",
      "01J84X5Y9R51A1S9Z1PRQNA68G": "No.5",
    },
    civic: {
      "01J6713KFB130HREHPZS32CYH5": "No.1",
      "01J66SCC2HREDJ2FFAZB4B7FB0": "No.2",
      "01J80QPKB96YHRRQVMYDBEKGTK": "No.3",
      "01J80QTZCJRJ3SF8E4P9Z33NZJ": "No.4",
      "01J80QVQ8SP7H1WR2BV10KG93V": "No.5",
      "01J80QWBMRY186P8CZQH2TER5W": "No.6",
      "01J84X50GKK1C6H5CEJ77SQPXH": "No.7",
    },
    "type-r": {
      "01J672CDTV49R3ZZ360X7KHAPY": "No.1",
      "01J672BSGV6RD1P8BZRTQJAHH3": "No.2",
      "01J672B3QCC56KGV05X2VREBCC": "No.3",
      "01J80R07CTT80P264WHQ3R6FWP": "No.4",
      "01J80R1RHXBPBYNJ4BFP12FF7S": "No.5",
      "01J84X81KG1H4RKKJWPBYXJ4Z0": "No.6",
    },
    "wr-v": {
      "01J672PBRHRJZ4JFYDGY6QY0ER": "No.1",
      "01J672NSKYS0R0SQMGF4NWH6QZ": "No.2",
      "01J672MJTNC4G54MHWT6Z7FBQV": "No.3",
      "01J80R35ME4V2M4CHWK70CXN14": "No.4",
      "01J80R3TC500HBFGGFJ4KKJWZ6": "No.5",
      "01J80R4GCE70Q2ETBNQB2FR3G2": "No.6",
    },
};
  
const PROD_TEXTURE_MAP = {
    nbox: {
      "01J6D0B6HH0340DE5C5D1PERGA": "No.1",
      "01J6D0A60PVJTBM6A7GBPXT6G6": "No.2",
      "01J85TWYGF7EBKAJM7706TZTEY": "No.3",
      "01J85TY0CXE93N8QBKBN03ZSYW": "No.4",
      "01J85TYP2MSMZ2FRYH7A3BF1VA": "No.5",
    },
    civic: {
      "01J6D00X0XFDVD0K6YN7KMC3G3": "No.1",
      "01J6D009Y953YBMKV8FHQ7MARP": "No.2",
      "01J85TQJFF84FM008VA918BKYY": "No.3",
      "01J85TRJY7Z5BZDX9K7WDMH806": "No.4",
      "01J85TSCBZ6KE6A52D0MM0F7WA": "No.5",
      "01J85TTJQF8AFQNN04TMNM5CGG": "No.6",
      "01J85TVEJ47774XR7AET6CN7F0": "No.7",
    },
    "type-r": {
      "01J6CZYH61E961DWDK0VA016FN": "No.1",
      "01J6CZXSAAAHA2JHQD06D813XY": "No.2",
      "01J6CZNW74T5JNF6NFY78H0AJF": "No.3",
      "01J85V05R9ZAV3DVD60W0A52TE": "No.4",
      "01J85V0W6CJQ1TPXAXNWA4TH7X": "No.5",
      "01J85V1GS4X39R8VXE8VG1R4BV": "No.6",
    },
    "wr-v": {
      "01J6D03SZB6MY5864C3KPZRFDZ": "No.1",
      "01J6D02ZF7XGAVCKQZNC0K6TEN": "No.2",
      "01J6D027YPK0DDTFS2D5WWXCR4": "No.3",
      "01J85V2HMES6FDJ3W42SZC6XZE": "No.4",
      "01J85V34JJQ0BVSQD27NQVAHA9": "No.5",
      "01J85V446GSXTRKEGNPNG9T91X": "No.6",
    },
};

const CAR_LICENSE_PLATE = {
    common__honda_mono: "HONDA MONO",
    common__honda_color: "HONDA",
    common__synergylab: "SYNERGY LAB",
    common__synergylab_black: "SYNERGY LAB BLACK",
    common__synergylab_white: "SYNERGY LAB WHITE",
    common__METAFORGE_black: "METAFORGE BLACK",
    common__METAFORGE_white: "METAFORGE WHITE",
    common__METAFORGE: "METAFORGE",
    common__PBD: "PBD",
    civic__black: "BLACK",
    civic__white: "WHITE",
    nbox__black: "BLACK",
    "type-r__black": "BLACK",
    "type-r__side": "SIDE",
    "type-r__white": "WHITE",
    "wr-v__black": "BLACK",
    "wr-v__white": "WHITE"
}

const CAR_NAME = {
                    nbox: "N-Box",
                    civic: "Civic",
                    "wr-v": "W-RV",
                    "type-r": "Type R",
                };

const CAR_INFO = {
                    nbox: {
                        name: "N-BOX CUSTOM",
                        model: "6BA-JF5", // 型式: 型式 is translated to 'model'
                        category: "Light Vehicle", // カテゴリー: カテゴリー is translated to 'category'
                        link: "https://www.honda.co.jp/Nbox/",
                    },
                    civic: {
                        name: "CIVIC e:HEV",
                        model: "6AA-FL4",
                        category: "Hatchback", // カテゴリー: カテゴリー is translated to 'category'
                        link: "https://www.honda.co.jp/CIVIC/",
                    },
                    "wr-v": {
                        name: "WR-V Z＋",
                        model: "5BA-DG5",
                        category: "SUV",
                        link: "https://www.honda.co.jp/WR-V/",
                    },
                    "type-r": {
                        name: "CIVIC TYPE R",
                        model: "6BA-FL5",
                        category: "Sports", // カテゴリー: カテゴリー is translated to 'category'
                        link: "https://www.honda.co.jp/CIVICTYPE-R/",
                    },
                };

const CAR_CONFIGS = {
    nbox: {
        grille: {
        name: "Grille",
        parts: [
            {
            id: "type1",
            name: "Type A", // タイプA: タイプA is translated to 'Type A'
            mesh: "Gril_Type01",
            },
            {
            id: "type2",
            name: "Type B", // タイプB: タイプB is translated to 'Type B'
            mesh: "Gril_Type02",
            },
            {
            id: "type3",
            name: "ORIGINAL", // オリジナル: オリジナル is translated to 'Original'
            mesh: "N-BOX_Custom_Parts",
            },
        ],
        },
        bumper: {
        name: "Bumper",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "Front_Bumper_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "Front_Bumper_Type02",
            },
        ],
        },
        side_garnish: {
        name: "Side・Rear Garnish",
        parts: [
            { id: "type0", name: "None", mesh: "" }, // なし: なし is translated to 'None'
            {
            id: "type2",
            name: "Installed", // あり: あり is translated to 'Installed'
            mesh: "Side_Garnish_Type02",
            },
        ],
        },
        tire: {
        name: "Wheels",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "TireWheel_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "TireWheel_Type02",
            },
            {
            id: "type3",
            name: "ORIGINAL",
            mesh: "TireWheel_Type03",
            },
        ],
        },
    },
    civic: {
        front_garnish: {
        name: "Front Garnish",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Installed",
            mesh: "CIVIC_Body_Option_Parts_Front_Garnish",
            },
        ],
        },
        rear_wing: {
        name: "Rear Wing",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Type A",
            mesh: "CIVIC_Body_Option_Rear_Wing",
            },
            {
            id: "type1",
            name: "ORIGINAL",
            mesh: "CIVIC_Custom_Parts",
            },
        ],
        },
        tire: {
        name: "Wheels",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "CIVIC_TireWheel_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "CIVIC_TireWheel_Type02",
            },
            {
            id: "type3",
            name: "ORIGINAL",
            mesh: "CIVIC_TireWheel_Type03",
            },
        ],
        },
    },
    "wr-v": {
        grille: {
        name: "Front Grille",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "WR-V_FrontGrille01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "WR-V_FrontGrille02",
            },
        ],
        },
        front_bumper: {
        name: "Front Bumper",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type3",
            name: "ORIGINAL",
            mesh: "WR-V_Custom_Parts",
            },
            // {
            //   id: "type2",
            //   name: "Installed",
            //   mesh: "WR-V_Option_Parts_Front",
            // },
        ],
        },
        front: {
        name: "Front Garnish",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Installed",
            mesh: "WR-V_Option_Parts_Front",
            },
        ],
        },
        side_garnish: {
        name: "Side Garnish",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Installed",
            mesh: "WR-V_Option_Parts_Side_Garnish",
            },
        ],
        },
        rear_garnish: {
        name: "Rear Garnish",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Installed",
            mesh: "WR-V_Option_Parts_Rear_Garnish",
            },
        ],
        },
        muffler: {
        name: "Muffler",
        parts: [
            { id: "type0", name: "None", mesh: "" },
            {
            id: "type2",
            name: "Installed",
            mesh: "WR-V_Option_Parts_muffler",
            },
        ],
        },
        tire: {
        name: "Wheels",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "WR-V_TireWheel",
            },
            {
            id: "type2",
            name: "ORIGINAL",
            mesh: "WR-V_TireWheel_Type02",
            },
        ],
        },
    },
    "type-r": {
        typer_bumper: {
        name: "Front Bumper",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "Type-R_Body_Front_Bumper_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "Type-R_Body_Front_Bumper_Type02_POTION_STAI",
            },
            {
            id: "type3",
            name: "ORIGINAL",
            mesh: "Type-R_Custom_Parts",
            },
        ],
        },
        typer_grille: {
        name: "Front Grille",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "Type-R_Front_Grille_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "Type-R_Front_Grille_Type02_OPTION_STAI",
            },
        ],
        },
        bonnet: {
        name: "Bonnet",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "Type-R_Bonnet_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "Type-R_Bonnet_Type02_OPTION",
            },
        ],
        },
        fender: {
        name: "Side Fender",
        parts: [
            { id: "type0", name: "Type A", mesh: "" },
            {
            id: "type2",
            name: "Type B",
            mesh: "Type-R_Body_Side_Fender_R_Type02_OPTION_STAI",
            mesh2: "Type-R_Body_Side_Fender_L_Type02_OPTION_STAI",
            },
        ],
        },
        typer_rear_wing: {
        name: "Rear Wing",
        parts: [
            {
            id: "type1",
            name: "Type A",
            mesh: "Type-R_Rear_Wing_Type01",
            },
            {
            id: "type2",
            name: "Type B",
            mesh: "Type-R_Rear_Wing_Type02",
          },
        ],
      },
      tire: {
        name: "Wheels",
        parts: [
          {
            id: "type1",
            name: "Type A",
            mesh: "Type-R_TireWheel",
          },
          {
            id: "type2",
            name: "ORIGINAL",
            mesh: "Type-R_TireWheel_Type02",
          },
        ],
      },
    },
  };

const findKeyByValue = (obj, value) => Object.entries(obj).find(([key, val]) => val === value)?.[0];

const findCarPartName = (carShortName, partName, partId) => {

    console.log("findCarPartName", carShortName, partName, partId);
    
    if(!partId)
        return "None";

    let newPartName;

    if(!CAR_CONFIGS[carShortName][partName] && partId) {
        newPartName = partId.toUpperCase();
        //newPartName = partId.charAt(0).toUpperCase() + partId.slice(1);
    }
    else {
        const part = CAR_CONFIGS[carShortName][partName].parts.find(part => part.id === partId);
        newPartName = part ? part.name : 'None';
    }

    return newPartName;
}

const findCarLicensePlate = (plate) => {

    console.log("findCarLicensePlate", plate);
    
    if(!plate)
        return "None";

    const plateValue = CAR_LICENSE_PLATE[plate]
    const newPlateValue = plateValue ? plateValue : 'None';

    return newPlateValue;
}

const findPaintName = (carShortName, textureURL, isTest) => {

    console.log("findPaintName", carShortName, textureURL, isTest);

    if(!textureURL)
        return 'None';

    let matches = textureURL.match(/image_([A-Z0-9]+)\.[a-zA-Z0-9]+$/);
    console.log("matches", matches);
    
    const textureArtworkId = matches[1];
    console.log("textureArtworkId", textureArtworkId);

    if(!textureArtworkId)
        return 'None'

    let paintName;

    if(isTest) {
        paintName = DEV_TEXTURE_MAP[carShortName][textureArtworkId];
    }
    else {
        paintName = PROD_TEXTURE_MAP[carShortName][textureArtworkId];
    }

    return paintName;
}

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

function checkIfFileExists(url) {
    return new Promise((resolve, reject) => {
      const req = https.request(url, { method: 'HEAD' }, (res) => {
        resolve(res.statusCode === 200); // Resolve to true if status code is 200, otherwise false
      });
  
      req.on('error', (err) => {
        reject(err); // Reject the promise if there's an error
      });
  
      req.end();
    });
}

export const handler = async (event) => {

    console.log("event", event);

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

            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + memberId }],}));
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        } 
        else if(!body.appPubKey && token) {
            //verify token
            try{
                const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key == 'JWT_SECRET').value);
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
        else {
            console.log("Invalid token.");
            return {
                Success: false,
                Code: 1,
                Message: "Invalid token."
            };
        }


        if(!body.nftType) {
            return {
                Success: false,
                Message: 'nftType is required'
            };
        }

        if(body.storeId === undefined){
            // return {
            //     Success: false, 
            //     Message: 'storeId is required',
            // };
            switch(body.nftType) {
                case 'CAR':
                    body.storeId = 'AURORA_CAR';
                    break;
                case 'CHARACTER':
                    body.storeId = 'AURORA_CHARACTER';
                    break;
                case 'MEMBER_A':
                    body.storeId = 'AURORA_MEMBERSHIP_A';
                    break;
                case 'MEMBER_B':
                    body.storeId = 'AURORA_MEMBERSHIP_B';
                    break;
                case 'RACINGFAN':
                    body.storeId = 'AURORA_RACINGFAN';
                    break;
                default:
                    break
            }
        }

        if(body.category === undefined){
            // return {
            //     Success: false, 
            //     Message: 'category is required',
            // };
            body.category = 'GRAPHIC'
        }

        if(body.licenseId === undefined) {
            body.licenseId = 'CC0';
        }

        if(body.royalty === undefined) {
            body.royalty = 0;
        }
        
        let sql;

        if(body.queueId) {
            sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , status = 'IN_PROGRESS' where PK = 'QUEUE#MINT#${body.queueId}' and SK = '${member.SK}'`;
            console.log("sql 2", sql);
            let updateQueueInProgressResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
            console.log("updateQueueInProgressResult", updateQueueInProgressResult);
        }

        let enumResult, membershipEnum, carCounterEnum, characterCounterEnum;
        let indexOfN100, N100Desc;  //, indexOfA3950, A3950Desc, indexOfA4000, A4000Desc, indexOfB3950, B3950Desc, indexOfB4000, B4000Desc;
        let indexOfN7900, N7900Desc, indexOfA, ADesc, indexOfB, BDesc, indexOfA1, A1Desc, indexOfB1, B1Desc, indexOfA2, A2Desc, indexOfB2, B2Desc, indexOfCount, countDesc;
        let membershipEnumModifiedDate, carEnumModifiedDate, characterEnumModifiedDate;
        const nextNftId = await getNextSequence("COUNTER", "NFT_SEQ");
        console.log("nextNftId", nextNftId);
        
        sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'MEMBERSHIP'`;
        enumResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        membershipEnum = enumResult.Items.map(unmarshall)[0];

        sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'RACING_FAN_NFT_SETTINGS'`;
        let rfSettingsResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        let rfSettings = rfSettingsResult.Items.map(unmarshall)[0];
        let rfSettingsModifiedDate = rfSettings.modified_date;
        rfSettings = JSON.parse(rfSettings.enum_values);
        console.log("rfSettings", rfSettings);
        
        
        console.log("initial membershipEnum", membershipEnum);




        indexOfN100 = membershipEnum.enum_values.split(',').indexOf('N-100');
        N100Desc = membershipEnum.enum_description.split(',')[indexOfN100];
        // indexOfA3950 = enumResult.enum_values.split(',').indexOf('A-3950');
        // A3950Desc = membershipEnum.enum_description.split(',')[indexOfA3950];
        // indexOfA4000 = enumResult.enum_values.split(',').indexOf('A-4000');
        // A4000Desc = membershipEnum.enum_description.split(',')[indexOfA4000];
        // indexOfB3950 = enumResult.enum_values.split(',').indexOf('B-3950');
        // B3950Desc = membershipEnum.enum_description.split(',')[indexOfB3950];
        // indexOfB4000 = enumResult.enum_values.split(',').indexOf('B-4000');
        // B4000Desc = membershipEnum.enum_description.split(',')[indexOfB4000];

        indexOfN7900 = membershipEnum.enum_values.split(',').indexOf('N-7900');
        N7900Desc = membershipEnum.enum_description.split(',')[indexOfN7900];
        indexOfA = membershipEnum.enum_values.split(',').indexOf('A');
        ADesc = membershipEnum.enum_description.split(',')[indexOfA];
        indexOfB = membershipEnum.enum_values.split(',').indexOf('B');
        BDesc = membershipEnum.enum_description.split(',')[indexOfB];
        indexOfA1 = membershipEnum.enum_values.split(',').indexOf('A1');
        A1Desc = membershipEnum.enum_description.split(',')[indexOfA1];
        indexOfB1 = membershipEnum.enum_values.split(',').indexOf('B1');
        B1Desc = membershipEnum.enum_description.split(',')[indexOfB1];
        indexOfA2 = membershipEnum.enum_values.split(',').indexOf('A2');
        A2Desc = membershipEnum.enum_description.split(',')[indexOfA2];
        indexOfB2 = membershipEnum.enum_values.split(',').indexOf('B2');
        B2Desc = membershipEnum.enum_description.split(',')[indexOfB2];
        indexOfCount = membershipEnum.enum_values.split(',').indexOf('COUNT');
        countDesc = membershipEnum.enum_description.split(',')[indexOfCount];

        membershipEnumModifiedDate = membershipEnum.modified_date;

        sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COUNTER_CHARACTER'`;
        enumResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        characterCounterEnum = enumResult.Items.map(unmarshall)[0];
        console.log("initial characterCounterEnum", characterCounterEnum);
        characterEnumModifiedDate = characterCounterEnum.modified_date;
        let characterDesc = characterCounterEnum.enum_description;

        sql = `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COUNTER_CAR'`;
        enumResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        carCounterEnum = enumResult.Items.map(unmarshall)[0];
        console.log("initial carCounterEnum", carCounterEnum);
        carEnumModifiedDate = carCounterEnum.modified_date;
        let carDesc = carCounterEnum.enum_description;


        
        let today = new Date().toISOString();
        let txStatements = [];

        let artwork, artworkV2, artworkV3;

        if(body.nftType == 'CAR' || body.nftType == 'CHARACTER') {    

            if(!body.artworkId) {
                console.log('artworkId is required');
                return {
                            Success: false,
                            Message: 'artworkId is required'
                        };
            }
            else {
                sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkId}'`;
                let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                artwork = artworkResult.Items.map(unmarshall)[0];
                let imgBase64;
                if(body.nftType == 'CHARACTER') {
                    imgBase64 = await getImageBase64(artwork.two_d_url);
                    body.nftURLBase64 = imgBase64;
                    body.fileName = artwork.two_d_file_name;
                }
                else if(body.nftType == 'CAR') {
                    
                    //todo : 20250712
                    // // artwork must be owned by current user
                    // if(artwork.user_id !== member.user_id) {
                    //     console.log("Car artwork must be owned by current user");
                        
                    //     return {
                    //         Success: false,
                    //         Message: '車のアートワークは現在のユーザーが所有する必要があります'
                    //     }
                    // }

                    let expectedTNFileName = `${artwork.artwork_id}_tn.png`;
                    let expectedTNUrl = `${configs.find(x => x.key =='S3_URL').value}/images/${expectedTNFileName}`;
                    console.log("expectedTNUrl", expectedTNUrl);
                    let isThumbnailExist = await checkIfFileExists(expectedTNUrl);
                    if(isThumbnailExist) {
                        imgBase64 = await getImageBase64(expectedTNUrl);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = expectedTNFileName;

                        sql = `update "${tableName}" set two_d_url_3 = '${expectedTNUrl}' , two_d_file_name_3 = '${expectedTNFileName}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
                        console.log("sql 1", sql);
                        
                        //txStatements.push({ "Statement": sql});
                        let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        console.log("updateArtworkResult for thumbnail", updateArtworkResult);
                    }
                    else {
                        imgBase64 = await getImageBase64(artwork.two_d_url_2);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name_2;
                    }
                }
                else {
                    throw new Error('Unexpected nftType');
                }

                if(body.nftType == 'CAR') {

                    // if(!member.survey_completed.includes(process.env.SURVEY_ID_A) && !member.survey_completed.includes(process.env.SURVEY_ID_B) && !member.survey_completed.includes(process.env.SURVEY_ID_B_CAR_MINTING)) {
                    //     console.log("Member not yet complete the related survey. メンバーは関連するアンケートをまだ完了していません。");
                    //     return {
                    //         Success: false,
                    //         Message: 'メンバーは関連するアンケートをまだ完了していません。'
                    //     }
                    // }

                    if(parseInt(carDesc) >= 8000) {
                        console.log("All MetaForge NFT have been minted . すべてのMETAFORGE NFTがミントされました。");
                        return {
                            Success: false,
                            Message: "すべてのMETAFORGE NFTがミントされました。"
                        }
                    }

                    if(member.nft_member_b_asset_name) {      

                        let ownerResult = await checkOwnerPost({
                            nftType: "MEMBER_B",
                            unit: member.nft_member_b_policy_id + member.nft_member_b_asset_name
                        }, headers['origin']);

                        if(ownerResult.Success) {
                            if(ownerResult.Data.owner != member.wallet_address){
                                console.log("User is not member of MetaForge. ユーザーはMETAFORGEのメンバーではありません。");
                                return {
                                    Success: false,
                                    Message: "ユーザーはMETAFORGEのメンバーではありません。"
                                }
                            }
                        }
                        else {
                            throw new Error('NFT の所有権を確認できません');   //Unable to check nft ownership
                        }
                    }
                    else {
                        console.log("User is not member of MetaForge. ユーザーはMETAFORGEのメンバーではありません。");
                        return {
                            Success: false,
                            Message: "ユーザーはMETAFORGEのメンバーではありません。"
                        }
                    }
                    
                    sql = `select * from "${tableName}"."InvertedIndex" where type = 'ASSET' and SK = '${member.SK}' and store_id = '${body.storeId}' and status = 'NOTFORSALE'`;
                    let existingAssetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    if(existingAssetResult.Items.length > 0) {

                        let membershipNFTsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${member.SK}' and type = 'ASSET' and asset_name = '${member.nft_member_b_asset_name}' and policy_id = '${member.nft_member_b_policy_id}' and status = 'NOTFORSALE'`}));
                        console.log("membershipNFTsResult", JSON.stringify(membershipNFTsResult));
                        if(membershipNFTsResult.Items.length === 0) {
                            return {
                                Success: false,
                                Message: 'Membership NFT not found.'
                            };
                        }

                        let BStatus;
                        let membershipNFTs = membershipNFTsResult.Items.map(unmarshall);
                        for (let i = 0; i < membershipNFTs.length; i++) {
                            const memberNFT = membershipNFTs[i];
                            console.log("memberNFT", memberNFT);
                            
                            if(memberNFT.store_id == 'AURORA_MEMBERSHIP_B') {
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
                                Message: 'MetaForge のメンバーシップ ランキングが無効です'    //Invalid membership ranking for MetaForge
                            };
                        }

                        if(member.SK == 'MEMBERWALLET#0x9b4380e74eCecf9B8c84393809A55c85D238fD3C') {
                            maxNFT = 9;
                        }
                        else if(member.SK == 'MEMBERWALLET#0x59D5993B49a44dAc3cd9911284E21746C97B55C0') {
                            maxNFT = 16;
                        }

                        if(existingAssetResult.Items.length + 1 > maxNFT) {
                            console.log("Maximum number of NFTs exceeded for your membership ranking. メンバーシップランキングのNFTの最大数を超えました。");
                            return {
                                Success: false,
                                Message: "メンバーシップランキングのNFTの最大数を超えました。"
                            }
                        }
                    }

                    // todo : 20250712
                    // if(!artwork.components) {
                    //     throw new Error("Missing car components");
                    // }

                    // let artworkComponents = JSON.parse(artwork.components);

                    // console.log("car", artwork.sub_category);
                    
                    // let _shortCarName = findKeyByValue(CAR_NAME, artwork.sub_category);
                    // console.log("_shortCarName", _shortCarName);
                    
                    let _metadata = [];

                    // let _metadata = [
                    //     {
                    //         "trait_type": "CarInfo Name",
                    //         "value": CAR_INFO[_shortCarName]["name"]
                    //     },
                    //     {
                    //         "trait_type": "CarInfo Model",
                    //         "value": CAR_INFO[_shortCarName]["model"]
                    //     },
                    //     {
                    //         "trait_type": "CarInfo Category",
                    //         "value": CAR_INFO[_shortCarName]["category"]
                    //     },
                    // ];

                    let _title;

                    // switch(_shortCarName) {
                    //     case "nbox":
                    //         _title = "NBOX";
                    //         _metadata = [..._metadata, 
                    //                                     {
                    //                                         "trait_type": "Custom Parts Grille",
                    //                                         "value": findCarPartName(_shortCarName, 'grille', (artworkComponents.find(item => item.grille !== undefined) || {}).grille)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Bumper",
                    //                                         "value": findCarPartName(_shortCarName, 'bumper', (artworkComponents.find(item => item.bumper !== undefined) || {}).bumper)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Side・Rear Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'side_garnish', (artworkComponents.find(item => item.side_garnish !== undefined) || {}).side_garnish)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Wheels",
                    //                                         "value": findCarPartName(_shortCarName, 'tire', (artworkComponents.find(item => item.tire !== undefined) || {}).tire)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Body",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Roof', (artworkComponents.find(item => item.color_Roof !== undefined) || {}).color_Roof)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Body",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Body', (artworkComponents.find(item => item.color_Body !== undefined) || {}).color_Body)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Bonnet', (artworkComponents.find(item => item.color_Bonnet !== undefined) || {}).color_Bonnet)
                    //                                     }
                    //                                 ];
                    //         break;
                    //     case "civic":
                    //         _title = "CIVIC";
                    //         _metadata = [..._metadata, 
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'front_garnish', (artworkComponents.find(item => item.front_garnish !== undefined) || {}).front_garnish)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Rear Wing",
                    //                                         "value": findCarPartName(_shortCarName, 'rear_wing', (artworkComponents.find(item => item.rear_wing !== undefined) || {}).rear_wing)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Wheels",
                    //                                         "value": findCarPartName(_shortCarName, 'tire', (artworkComponents.find(item => item.tire !== undefined) || {}).tire)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Body",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Roof', (artworkComponents.find(item => item.color_Roof !== undefined) || {}).color_Roof)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Body",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Body', (artworkComponents.find(item => item.color_Body !== undefined) || {}).color_Body)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Bonnet', (artworkComponents.find(item => item.color_Bonnet !== undefined) || {}).color_Bonnet)
                    //                                     }
                    //                                 ];
                    //         break;
                    //     case "wr-v":
                    //         _title = "WR-V";
                    //         _metadata = [..._metadata, 
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Grille",
                    //                                         "value": findCarPartName(_shortCarName, 'grille', (artworkComponents.find(item => item.grille !== undefined) || {}).grille)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Bumper",
                    //                                         "value": findCarPartName(_shortCarName, 'front_bumper', (artworkComponents.find(item => item.front_bumper !== undefined) || {}).front_bumper)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'front', (artworkComponents.find(item => item.front !== undefined) || {}).front)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Side Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'side_garnish', (artworkComponents.find(item => item.side_garnish !== undefined) || {}).side_garnish)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Rear Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'rear_garnish', (artworkComponents.find(item => item.rear_garnish !== undefined) || {}).rear_garnish)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Muffler",
                    //                                         "value": findCarPartName(_shortCarName, 'muffler', (artworkComponents.find(item => item.muffler !== undefined) || {}).muffler)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Wheels",
                    //                                         "value": findCarPartName(_shortCarName, 'tire', (artworkComponents.find(item => item.tire !== undefined) || {}).tire)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Body",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Fender Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Alum Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Body",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Body', (artworkComponents.find(item => item.color_Body !== undefined) || {}).color_Body)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Fender Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Resin_Garnish', (artworkComponents.find(item => item.color_Resin_Garnish !== undefined) || {}).color_Resin_Garnish)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Alum Garnish",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Alum_Garnish', (artworkComponents.find(item => item.color_Alum_Garnish !== undefined) || {}).color_Alum_Garnish)
                    //                                     }
                    //                                 ];
                    //         break;
                    //     case "type-r":
                    //         //[{"bonnet":"type1"},{"typer_rear_wing":"type1"},{"typer_bumper":"type1"},{"typer_grille":"type1"},{"material":"original"},{"fender":"type0"},{"tire":"type1"},{"color_Roof":"#151531"},{"color_Body":"#151531"},{"color_Bonnet":"#151531"},{"artworkId":"01J918J0Y6R4SSQC1RRG5F8ZE1"},{"customTexture":null},{"legitTexture":"https://s3.ap-northeast-1.amazonaws.com/anifie.community.resource/images/image_01J85V1GS4X39R8VXE8VG1R4BV.png"}]
                    //         //[{"artworkId":"01J6GZYYNWK0PW4H2VFRDHR0GH"},{"bonnet":"type2"},{"typer_rear_wing":"type2"},{"typer_bumper":"type3"},{"typer_grille":"type2"},{"material":"original"},{"fender":"type2"},{"tire":"type2"},{"color_Roof":"ffac14.66666668"},{"color_Body":"#ffaf00"},{"color_Bonnet":"ffac14.66666668"},{"mat_Body":"metallic"},{},{"mat_Bonnet":"matte"},{"mat_Roof":"matte"},{"customTexture":null},{"legitTexture":"https://s3.ap-northeast-1.amazonaws.com/anifie.community.resource/images/image_01J85V1GS4X39R8VXE8VG1R4BV.png"},{"plate":"common__METAFORGE_black"},{"tire_height":1},{"tire_offset":5}]
                    //         _title = "TYPE-R";
                    //         _metadata = [..._metadata, 
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Grille",
                    //                                         "value": findCarPartName(_shortCarName, 'typer_grille', (artworkComponents.find(item => item.typer_grille !== undefined) || {}).typer_grille)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Front Bumper",
                    //                                         "value": findCarPartName(_shortCarName, 'typer_bumper', (artworkComponents.find(item => item.typer_bumper !== undefined) || {}).typer_bumper)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'bonnet', (artworkComponents.find(item => item.bonnet !== undefined) || {}).bonnet)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Side Fender",
                    //                                         "value": findCarPartName(_shortCarName, 'fender', (artworkComponents.find(item => item.fender !== undefined) || {}).fender)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Rear Wing",
                    //                                         "value": findCarPartName(_shortCarName, 'typer_rear_wing', (artworkComponents.find(item => item.typer_rear_wing !== undefined) || {}).typer_rear_wing)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Custom Parts Wheels",
                    //                                         "value": findCarPartName(_shortCarName, 'tire', (artworkComponents.find(item => item.tire !== undefined) || {}).tire)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Body",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Material Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'material', (artworkComponents.find(item => item.material !== undefined) || {}).material)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Body",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Body', (artworkComponents.find(item => item.color_Body !== undefined) || {}).color_Body)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Roof",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Roof', (artworkComponents.find(item => item.color_Roof !== undefined) || {}).color_Roof)
                    //                                     },
                    //                                     {
                    //                                         "trait_type": "Color Bonnet",
                    //                                         "value": findCarPartName(_shortCarName, 'color_Bonnet', (artworkComponents.find(item => item.color_Bonnet !== undefined) || {}).color_Bonnet)
                    //                                     }
                    //                                 ];
                    //         break;
                    //     default:
                    //         throw new Error("Unexpected car category: " + _shortCarName);
                    // }

                    let likeStampsCount = [];

                    sql = `select * from "${tableName}" where PK = 'ARTWORKFAVOURITE#${artwork.artwork_id}' and type = 'ARTWORKFAVOURITE' `;
                    let artworkFavResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    if(artworkFavResult.Items.length > 0) {
                        let artworkFavs = artworkFavResult.Items.map(unmarshall);
                        let likeStamps = artworkFavs.map(x => x.like_stamp).filter(onlyUnique);   
                        for (let i = 0; i < likeStamps.length; i++) {
                            const stamp = likeStamps[i];
                            if(stamp === undefined || stamp === '')
                                continue;

                            let currentStampCount = artworkFavs.filter(x => x.like_stamp === stamp).length;
                            likeStampsCount.push({Stamp: stamp, Count: currentStampCount})
                        }
                    }

                    console.log("likeStampsCount", likeStampsCount);
                    
                    // todo: 20250712
                    // _metadata = [
                    //                 ..._metadata,
                    //                 {
                    //                     "trait_type": "Paint Name",
                    //                     "value": findPaintName(_shortCarName, (artworkComponents.find(item => item.legitTexture !== undefined) || {}).legitTexture, tableName == process.env.TABLE_NAME_TEST)
                    //                 },
                    //                 {
                    //                     "trait_type": "License Plate Name",
                    //                     "value": findCarLicensePlate((artworkComponents.find(item => item.plate !== undefined) || {}).plate)
                    //                     //"value": (artworkComponents.find(item => item.plate !== undefined) || {}).plate || 'None'
                    //                 },
                    //                 {
                    //                     "trait_type": "Number Of Stamps (ALL)",
                    //                     "value": likeStampsCount.reduce((sum, x) => sum + x.Count, 0)
                    //                 },
                    //                 {
                    //                     "trait_type": "Number Of Stamps (Cool)",
                    //                     "value": likeStampsCount.find(x => x.Stamp == 'COOL')?.Count ?? 0
                    //                 },
                    //                 {
                    //                     "trait_type": "Number Of Stamps (Cute)",
                    //                     "value": likeStampsCount.find(x => x.Stamp == 'CUTE')?.Count ?? 0
                    //                 },
                    //                 {
                    //                     "trait_type": "Number Of Stamps (Like)",
                    //                     "value": likeStampsCount.find(x => x.Stamp == 'LIKE')?.Count ?? 0
                    //                 },
                    //                 {
                    //                     "trait_type": "Number Of Stamps (God)",
                    //                     "value": likeStampsCount.find(x => x.Stamp == 'GOD')?.Count ?? 0
                    //                 }
                    //             ];
                    
                    body.metadata = _metadata;

                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${parseInt(carDesc) + 1}' where PK = '${carCounterEnum.PK}' and SK = '${carCounterEnum.SK}' and modified_date = '${carEnumModifiedDate}'`;
                    txStatements.push({ "Statement": sql});

                    body.name = `METAFORGE 2025 ${_title ? _title : ""} #${nextNftId}`;   //"MetaForge #" + (parseInt(carDesc) + 1);
                    body.description = 'Car "METAFORGE 2025" hosted by Aurora Synex Co., Ltd.'; 
                    // body.name = artwork.name; // can't use artwork name, too long to be accepted by Cardano
                    // body.description = artwork.description;  // can't use artwork description, too long to be accepted by Cardano
                }
                else if (body.nftType == 'CHARACTER') {

                    // if(!member.survey_completed.includes(process.env.SURVEY_ID_A) && !member.survey_completed.includes(process.env.SURVEY_ID_B)) {
                    //     console.log("Member not yet complete the related survey. メンバーは関連するアンケートをまだ完了していません。");
                    //     return {
                    //         Success: false,
                    //         Message: 'メンバーは関連するアンケートをまだ完了していません。'
                    //     }
                    // }

                    if(parseInt(characterDesc) >= 8000) {
                        console.log("All LittleBlue NFT have been minted . すべてのLittleBlue NFTがミントされました。");
                        return {
                            Success: false,
                            Message: "すべてのLittleBlue NFTがミントされました。"
                        }
                    }

                    if(!member.discord_roles.includes('LITTLEBLUE')){
                        console.log("User is not member of LittleBlue ユーザーはLittleBlueのメンバーではありません。");
                        return {
                            Success: false,
                            Message: "ユーザーはLittleBlueのメンバーではありません。" 
                        }
                    }

                    if(member.nft_member_a_asset_name) {      
                                          
                        let ownerResult = await checkOwnerPost({
                            nftType: "MEMBER_A",
                            unit: member.nft_member_a_policy_id + member.nft_member_a_asset_name
                        }, headers['origin']);

                        if(ownerResult.Success) {
                            if(ownerResult.Data.owner != member.wallet_address){
                                console.log("User is not member of LittleBlue ユーザーはLittleBlueのメンバーではありません。");
                                return {
                                    Success: false,
                                    Message: "ユーザーはLittleBlueのメンバーではありません。"
                                }
                            }
                        }
                        else {
                            throw new Error('NFT の所有権を確認できません');   //Unable to check nft ownership
                        }
                    }
                    else {
                        console.log("User is not member of LittleBlue ユーザーはLittleBlueのメンバーではありません。");
                        return {
                            Success: false,
                            Message: "ユーザーはLittleBlueのメンバーではありません。"
                        }
                    }

                    let memberWhitelistResult = await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST'`}));
                    console.log("memberWhitelistResult", memberWhitelistResult);
                    let memberWhiteLists = memberWhitelistResult.Items.map(unmarshall);
                    if(memberWhiteLists.find(x => x.whitelist_type.includes('LITTLEBLUE_ADDITIONAL_NFT'))) {
                        // allow  multiple LittleBlue content NFT
                    }
                    else {
                        sql = `select * from "${tableName}"."InvertedIndex" where type = 'ASSET' and SK = '${member.SK}' and store_id = '${body.storeId}' and status = 'NOTFORSALE'`;
                        let exitingAssetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        if(exitingAssetResult.Items.length > 0) {
                            console.log("User already requested LittleBlue NFT. ユーザーはすでにLittleBlue NFTをリクエストしています。");
                            return {
                                Success: false,
                                Message: "ユーザーはすでにLittleBlue NFTをリクエストしています。"
                            }
                        }
                    }

                    

                    let _metadata = [
                        {
                            "trait_type": "Project",
                            "value": "LittleBlue"
                        },
                        {
                            "trait_type": "Illustrator",
                            "value": "ミズノシンヤ" // Shinya Mizuno
                        },
                    ];
    
                    if(artwork.metadata) {
                        let artworkMetadata = JSON.parse(artwork.metadata);
                        if(artworkMetadata.length > 0)
                            _metadata = _metadata.concat(artworkMetadata);
                    }
                    
                    body.metadata = _metadata;

                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${parseInt(characterDesc) + 1}' where PK = '${characterCounterEnum.PK}' and SK = '${characterCounterEnum.SK}' and modified_date = '${characterEnumModifiedDate}'`;
                    txStatements.push({ "Statement": sql});

                    //body.name = "LittleBlue #" + (parseInt(characterDesc) + 1);
                    if(artwork.artwork_id == process.env.ARTWORK_ID_BLUE_HAIR_CHARACTER) {
                        body.name = `Spica #${nextNftId}`;
                        body.description = 'An intelligent and calm alien from "LittleBlue"';
                    }
                    else if(artwork.artwork_id == process.env.ARTWORK_ID_RED_HAIR_CHARACTER) {
                        body.name = `Capella #${nextNftId}`;
                        body.description = 'A lively and energetic alien from "LittleBlue"';
                    }
                }

            }

            if(body.artworkId2) {
                sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkId2}'`;
                let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                artworkV2 = artworkResult.Items.map(unmarshall)[0];
            }

            if(body.artworkId3) {
                sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${body.artworkId3}'`;
                let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                artworkV3 = artworkResult.Items.map(unmarshall)[0];
            }
        }
        else if(body.nftType === 'MEMBER_A') {
            
            if(member.nft_member_a_asset_name) {
                console.log('Member already own MEMBERSHIP NFT for project LittleBlue ' + member.userId  + " メンバーはすでにプロジェクトLittleBlueのメンバーシップNFTを所有しています。");
                return {
                    Success: false,
                    Message: 'メンバーはすでにプロジェクトLittleBlueのメンバーシップNFTを所有しています。'
                }
            }

            // let attempt = 0;
            // while(true) {
                
            //     if(member.survey_completed)
            //         break;

            //     let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = '${member.PK}' and SK = '${member.SK}' and type = 'MEMBER'`}))
            //     console.log("memberResult", JSON.stringify(memberResult));
            //     if(memberResult.Items.length === 0) {
            //         return {
            //             Success: false,
            //             Message: 'member not found',
            //         };
            //     }
            //     member = memberResult.Items.map(unmarshall)[0];

            //     await new Promise(r => setTimeout(r, 10000));   // sleep 10 seconds
            //     if(attempt >= 6) {
            //         throw new Error('Retry get survey_completed exceeded max retry attempt');
            //     }
            //     attempt ++;
            // }

            // if(!member.survey_completed.includes(process.env.SURVEY_ID_A) && !member.survey_completed.includes(process.env.SURVEY_ID_B)) {
            //     console.log("Member not yet complete the related survey. メンバーは関連するアンケートをまだ完了していません。");
            //     return {
            //         Success: false,
            //         Message: 'メンバーは関連するアンケートをまだ完了していません。'
            //     }
            // }

            if(member.campaign_code) {
                console.log("campaign code", member.campaign_code);

                sql = `SELECT * FROM "${tableName}" WHERE PK = 'CAMPAIGNCODE'`;
                let campaignCodeResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                let campaignCodes = campaignCodeResult.Items.map(unmarshall);
                let foundCampaignCode = campaignCodes.find(x => x.code == member.campaign_code);
                if(foundCampaignCode && !foundCampaignCode.used_by && campaignCodes.find(x => x.used_by === member.user_id) == undefined) {
                    body.campaignCode = member.campaign_code;    
                }
            }

            if(today > '2024-07-01T15:00:00.000Z') {    // '2025-07-14T15:00:00.000Z'
                
                if(body.campaignCode && !body.forcePreregister && !body.forceRegular) {  

                    console.log("Member A " + member.user_id + ' register within 6/18 to 7/10');

                    if(N100Desc !== 'N-100/100') {  // if N100 not yet full

                        if(member.nft_member_b_asset_name) {
                            sql = `select * from "${tableName}" where PK = 'ASSET#${member.nft_member_b_policy_id}#${member.nft_member_b_asset_name}' and SK = 'MEMBERWALLET#${member.wallet_address}' and type = 'ASSET'`;
                            let assetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                            if(assetResult.Items.length == 0) {
                                throw new Error('membership NFT not found. asset name: ' + member.nft_member_b_asset_name);
                            }
                            let asset = assetResult.Items.map(unmarshall)[0];
                            console.log("asset B", asset);
                            let _metadata = JSON.parse(asset.metadata);
                            console.log("metadata nft B", _metadata);
                            let rarity = _metadata.attributes.find(x => x.trait_type == "Rarity");
                            console.log("rarity", rarity);
                            if(!rarity) {
                                throw new Error("Missing rarity Attrribute for membership NFT aseset name " + member.nft_member_b_asset_name);
                            }
                            if(rarity.value === 'Legend') {
                                console.log('Simultaneous acquisition of campaign membership NFT for A and B is not allowed. ' + member.userId + " . LittleBlueとMETAFORGEのキャンペーンメンバーシップNFTを同時に取得することはできません。");
                                return {
                                    Success: false,
                                    Message: 'LittleBlueとMETAFORGEのキャンペーンメンバーシップNFTを同時に取得することはできません。'
                                }
                            }
                        }

                        // validate campaign code
                        sql = `SELECT * FROM "${tableName}" WHERE PK = 'CAMPAIGNCODE'`;
                        let campaignCodeResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        let campaignCodes = campaignCodeResult.Items.map(unmarshall);
                        let foundCampaignCode = campaignCodes.find(x => x.code == body.campaignCode);
                        if(!foundCampaignCode) {
                            console.log("Invalid campaign code " + body.campaignCode + " . 無効なキャンペーンコードです。");
                            return {
                                Success: false,
                                Message: '無無効なキャンペーンコードです。 ' + body.campaignCode
                            }
                        }
                        
                        if(foundCampaignCode.used_by) {
                            console.log("Campaign code is already been used " + body.campaignCode + " キャンペーンコードはすでに使用されています。");
                            return {
                                Success: false,
                                Message: 'キャンペーンコードはすでに使用されています。 ' + body.campaignCode
                            }
                        }

                        if(campaignCodes.find(x => x.used_by === member.user_id) !== undefined) {
                            console.log('Member already have Campaign Membership NFT. メンバーはすでにキャンペーンメンバーシップNFTを持っています。');
                            return {
                                Success: false,
                                Message: 'メンバーはすでにキャンペーンメンバーシップNFTを持っています。'
                            }
                        }

                        sql = `update "${tableName}" set used_by = '${member.user_id}' , project = 'LITTLEBLUE', modified_date = '${new Date().toISOString()}' where PK = '${foundCampaignCode.PK}' and SK = '${foundCampaignCode.SK}'`;
                        txStatements.push({ "Statement": sql});
    
                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_A_CAMPAIGN_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;
    
                        //increment counter for N100
                        let currentCountN100 = parseInt(N100Desc.replace('N-', '').split('/')[0]);
                        let newN100Desc = `N-${currentCountN100 + 1}/100`;
                        let currentCountA1 = parseInt(A1Desc.replace('A1-', ''));
                        let newA1Desc = `A1-${currentCountA1 + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${newN100Desc},${N7900Desc},${ADesc},${BDesc},${newA1Desc},${B1Desc},${A2Desc},${B2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;
                        txStatements.push({ "Statement": sql});

                        // body.name = "Member #" + newCountDesc;
                        //body.description = 'A Campaign Membership NFT';
                        body.name = `LittleBlue Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: 'ch-web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };            
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            // {
                            //     "trait_type": "Publisher",
                            //     "value": "Aurora Synex Co., Ltd."
                            // },
                            {
                                "trait_type": "Community",
                                "value": "LittleBlue"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Legend"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Innovator"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;
    
                    }
                    else {
                        console.log("All NFT are fully minted for first 100th NFT . 最初の100枚のNFTはすべてミントされています。");
                        return {
                            Success: false,
                            Message: "最初の100枚のNFTはすべてミントされています。"
                        }
                    }
                }
                else if((member.created_date >= '2025-06-18T04:30:00.000Z' && member.created_date < '2025-07-14T15:00:00.000Z') || (body.forcePreregister === true && body.appPubKey === undefined)) {    // 6/18 to 7/15 JST //only admin can forcePreregister

                    console.log("Member A " + member.user_id + ' register within 6/18 to 7/14');

                    if(N7900Desc != 'N-7900/7900') {  // if N7900 not yet full

                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_A_PRE_REGISTER_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;
        
                        //increment counter for N7900
                        let currentCountN7900 = parseInt(N7900Desc.replace('N-', '').split('/')[0]);
                        let newN7900Desc = `N-${currentCountN7900 + 1}/7900`;
                        let currentCountA2 = parseInt(A2Desc.replace('A2-', ''));
                        let newA2Desc = `A2-${currentCountA2 + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${N100Desc},${newN7900Desc},${ADesc},${BDesc},${A1Desc},${B1Desc},${newA2Desc},${B2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;

                        txStatements.push({ "Statement": sql});

                        // body.name = "Member #" + newCountDesc;
                        // body.description = 'A Pre-register Membership NFT';
                        body.name = `LittleBlue Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: 'ch-web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };            
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            // {
                            //     "trait_type": "Publisher",
                            //     "value": "Aurora Synex Co., Ltd."
                            // },
                            {
                                "trait_type": "Community",
                                "value": "LittleBlue"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Common"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Innovator"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;
                    }
                    else {
                        console.log("Pre-registration NFT for LittleBlue is fully minted . LittleBlueの事前登録NFTはすべてミントされています。");

                        return {
                            Success: false,
                            Message: "LittleBlueの事前登録NFTはすべてミントされています。"
                        }
                    }
                }
                else if((member.created_date >= '2025-07-14T15:00:00.000Z' && member.created_date < '2025-10-31T15:00:00.000Z') || (body.forceRegular === true && body.appPubKey === undefined)) {    // 7/15 to 8/16 JST //only admin can forceRegular

                    console.log("Member A " + member.user_id + ' register within 7/15 to 10/31');

                    let currentCountA = parseInt(ADesc.replace('A-', ''));
                    let currentCountA1 = parseInt(A1Desc.replace('A1-', ''));
                    let currentCountA2 = parseInt(A2Desc.replace('A2-', ''));

                    let Alimit = 8000-currentCountA1-currentCountA2;

                    if(currentCountA < Alimit) {  // if A regular not yet full
        
                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_A_POST_REGISTER_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;
        
                        //increment counter for A
                        let currentCountA = parseInt(ADesc.replace('A-', ''));
                        let newADesc = `A-${currentCountA + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${N100Desc},${N7900Desc},${newADesc},${BDesc},${A1Desc},${B1Desc},${A2Desc},${B2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;
                        txStatements.push({ "Statement": sql});

                        // body.name = "Member #" + newCountDesc;
                        // body.description = 'A Regular Membership NFT';
                        body.name = `LittleBlue Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: process.env.LAMBDA_PREFIX + 'web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };            
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            {
                                "trait_type": "Community",
                                "value": "LittleBlue"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Common"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Associate"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;
                    }
                    else {
                        console.log("All NFT are fully minted for project LittleBlue プロジェクトLittleBlueのNFTはすべてミントされています。");
                        return {
                            Success: false,
                            Message: "プロジェクトLittleBlueのNFTはすべてミントされています。"
                        }
                    }
                }
                else {
                    console.log('Only member register between 6/18 and 10/31 are eligible for LittleBlue NFT . 6/18から10/31の間に登録したメンバーのみがLittleBlue NFTの対象となります。');
                    return {
                        Success: false,
                        Message: '6/18から10/31の間に登録したメンバーのみがLittleBlue NFTの対象となります。'
                    }
                }
            }
            else {

                console.log("NFT Minting will start after 7/15 . NFTのミントは7/15以降に開始されます。");
                return {
                    Success: false,
                    Message: "NFTのミントは7/15以降に開始されます。"
                }
            }
        }
        else if(body.nftType === 'MEMBER_B') {
            
            if(member.nft_member_b_asset_name) {
                console.log('Member already own Membership NFT for project MetaForge. ' + member.userId + " . メンバーはすでにプロジェクトMETAFORGEのメンバーシップNFTを所有しています。");
                return {
                    Success: false,
                    Message: 'メンバーはすでにプロジェクトMETAFORGEのメンバーシップNFTを所有しています。'
                }
            }

            // let attempt = 0;
            // while(true) {
                
            //     if(member.survey_completed)
            //         break;

            //     let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = '${member.PK}' and SK = '${member.SK}' and type = 'MEMBER'`}))
            //     console.log("memberResult", JSON.stringify(memberResult));
            //     if(memberResult.Items.length === 0) {
            //         return {
            //             Success: false,
            //             Message: 'member not found',
            //         };
            //     }
            //     member = memberResult.Items.map(unmarshall)[0];

            //     await new Promise(r => setTimeout(r, 10000));   // sleep 10 seconds
            //     if(attempt >= 6) {
            //         throw new Error('Retry get survey_completed exceeded max retry attempt');
            //     }
            //     attempt ++;
            // }

            // if(!member.survey_completed.includes(process.env.SURVEY_ID_A) && !member.survey_completed.includes(process.env.SURVEY_ID_B)) {
            //     console.log("Member not yet complete the related survey. メンバーは関連するアンケートをまだ完了していません。");
            //     return {
            //         Success: false,
            //         Message: 'メンバーは関連するアンケートをまだ完了していません。'
            //     }
            // }

            if(member.campaign_code) {
                console.log("campaign code", member.campaign_code);

                sql = `SELECT * FROM "${tableName}" WHERE PK = 'CAMPAIGNCODE'`;
                let campaignCodeResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                let campaignCodes = campaignCodeResult.Items.map(unmarshall);
                let foundCampaignCode = campaignCodes.find(x => x.code == member.campaign_code);
                if(foundCampaignCode && !foundCampaignCode.used_by && campaignCodes.find(x => x.used_by === member.user_id) == undefined) {
                    body.campaignCode = member.campaign_code;    
                }
            }
            
            if(today > '2024-07-01T15:00:00.000Z') {

                if(body.campaignCode && !body.forcePreregister && !body.forceRegular) {  

                    console.log("Member B " + member.user_id + ' register within 6/18 to 7/10');

                    if(N100Desc !== 'N-100/100') {
                        
                        if(member.nft_member_a_asset_name) {
                            sql = `select * from "${tableName}" where PK = 'ASSET#${member.nft_member_a_policy_id}#${member.nft_member_a_asset_name}' and SK = 'MEMBERWALLET#${member.wallet_address}' and type = 'ASSET'`;
                            let assetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                            if(assetResult.Items.length == 0) {
                                throw new Error('membership NFT not found. asset name: ' + member.nft_member_a_asset_name);
                            }
                            let asset = assetResult.Items.map(unmarshall)[0];
                            console.log("asset A", asset);
                            let _metadata = JSON.parse(asset.metadata);
                            console.log("metadata nft A", _metadata);
                            let rarity = _metadata.attributes.find(x => x.trait_type == "Rarity");
                            console.log("rarity", rarity);
                            if(!rarity) {
                                throw new Error("Missing rarity Attrribute for membership NFT asset name " + member.nft_member_a_asset_name);
                            }
                            if(rarity.value === 'Legend') {
                                console.log('Simultaneous acquisition of campaign membership NFT for A and B is not allowed. ' + member.userId + " . LittleBlueとMETAFORGEのキャンペーンメンバーシップNFTを同時に取得することはできません。");
                                return {
                                    Success: false,
                                    Message: 'LittleBlueとMETAFORGEのキャンペーンメンバーシップNFTを同時に取得することはできません。'
                                }
                            }
                        }

                        // validate campaign code
                        sql = `SELECT * FROM "${tableName}" WHERE PK = 'CAMPAIGNCODE'`;
                        let campaignCodeResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        let campaignCodes = campaignCodeResult.Items.map(unmarshall);
                        let foundCampaignCode = campaignCodes.find(x => x.code == body.campaignCode);
                        if(!foundCampaignCode) {
                            console.log("Invalid campaign code " + body.campaignCode + " . 無効なキャンペーンコードです。");
                            return {
                                Success: false,
                                Message: '無効なキャンペーンコードです。 ' + body.campaignCode
                            }
                        }
                        
                        if(foundCampaignCode.used_by) {
                            console.log("Campaign code is already been used " + body.campaignCode + ' . キャンペーンコードはすでに使用されています。');
                            return {
                                Success: false,
                                Message: 'キャンペーンコードはすでに使用されています。 ' + body.campaignCode
                            }
                        }

                        if(campaignCodes.find(x => x.used_by === member.user_id) !== undefined) {
                            console.log('Member already have Campaign Membership NFT . メンバーはすでにキャンペーンメンバーシップNFTを持っています。');
                            return {
                                Success: false,
                                Message: 'メンバーはすでにキャンペーンメンバーシップNFTを持っています。'
                            }
                        }
    
                        sql = `update "${tableName}" set used_by = '${member.user_id}' , project = 'METAFORGE' , modified_date = '${new Date().toISOString()}' where PK = '${foundCampaignCode.PK}' and SK = '${foundCampaignCode.SK}'`;
                        txStatements.push({ "Statement": sql});
    
                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_B_CAMPAIGN_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;
    
                        //increment counter for N100
                        let currentCountN100 = parseInt(N100Desc.replace('N-', '').split('/')[0]);
                        let newN100Desc = `N-${currentCountN100 + 1}/100`;
                        let currentCountB1 = parseInt(B1Desc.replace('B1-', ''));
                        let newB1Desc = `B1-${currentCountB1 + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${newN100Desc},${N7900Desc},${ADesc},${BDesc},${A1Desc},${newB1Desc},${A2Desc},${B2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;

                        // body.name = "Member #" + newCountDesc;
                        // body.description = 'B Campaign Membership NFT';
                        body.name = `METAFORGE Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        txStatements.push({ "Statement": sql});

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: process.env.LAMBDA_PREFIX + 'web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };            
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            // {
                            //     "trait_type": "Publisher",
                            //     "value": "Aurora Synex Co., Ltd."
                            // },
                            {
                                "trait_type": "Community",
                                "value": "MetaForge"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Legend"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Innovator"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;
                    }
                    else {
                        console.log("All NFT are fully minted for first 100th NFT . 最初の100枚のNFTはすべてミントされています。");
                        return {
                            Success: false,
                            Message: "最初の100枚のNFTはすべてミントされています。"
                        }
                    }
                }
                else if((member.created_date >= '2025-06-18T04:30:00.000Z' && member.created_date < '2025-07-14T15:00:00.000Z') || (body.forcePreregister === true && body.appPubKey === undefined)) {    // 6/18 to 7/15 JST //only admin can forcePreregister

                    console.log("Member A " + member.user_id + ' register within 6/18 to 7/14');

                    if(N7900Desc != 'N-7900/7900') {  // if N7900 not yet full

                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_B_PRE_REGISTER_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;

                        //increment counter for N7900
                        let currentCountN7900 = parseInt(N7900Desc.replace('N-', '').split('/')[0]);
                        let newN7900Desc = `N-${currentCountN7900 + 1}/7900`;
                        let currentCountB2 = parseInt(B2Desc.replace('B2-', ''));
                        let newB2Desc = `B2-${currentCountB2 + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${N100Desc},${newN7900Desc},${ADesc},${BDesc},${A1Desc},${B1Desc},${A2Desc},${newB2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;
                        txStatements.push({ "Statement": sql});  
                        
                        // body.name = "Member #" + newCountDesc;
                        // body.description = 'B Pre-register Membership NFT';
                        body.name = `METAFORGE Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: process.env.LAMBDA_PREFIX + 'web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };            
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            {
                                "trait_type": "Publisher",
                                "value": "Aurora Synex Co., Ltd."
                            },
                            {
                                "trait_type": "Community",
                                "value": "MetaForge"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Common"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Innovator"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;
                    }
                    else {
                        console.log("Pre-registration NFT for MetaForge is fully minted . METAFORGEの事前登録NFTはすべてミントされています。");

                        return {
                            Success: false,
                            Message: "METAFORGEの事前登録NFTはすべてミントされています。"
                        }
                    }
                }
                else if((member.created_date >= '2025-07-14T15:00:00.000Z' && member.created_date < '2025-10-31T15:00:00.000Z') || (body.forceRegular === true && body.appPubKey === undefined)) {    // 7/15 to 8/16 JST //only admin can forceRegular

                    console.log("Member B " + member.user_id + ' register within 7/15 to 10/31');

                    let currentCountB = parseInt(BDesc.replace('B-', ''));
                    let currentCountB1 = parseInt(B1Desc.replace('B1-', ''));
                    let currentCountB2 = parseInt(B2Desc.replace('B2-', ''));

                    let Blimit = 8000-currentCountB1-currentCountB2;

                    if(currentCountB < Blimit) {  // if B regular not yet full
        
                        sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_B_POST_REGISTER_ARTWORK_ID}'`;
                        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                        artwork = artworkResult.Items.map(unmarshall)[0];
                        let imgBase64 = await getImageBase64(artwork.two_d_url);
                        body.nftURLBase64 = imgBase64;
                        body.fileName = artwork.two_d_file_name;
        
                        //increment counter for B
                        let currentCountB = parseInt(BDesc.replace('B-', ''));
                        let newBDesc = `B-${currentCountB + 1}`;
                        let currentCount = parseInt(countDesc);
                        let newCountDesc = `${currentCount + 1}`;
                        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_description = '${N100Desc},${N7900Desc},${ADesc},${newBDesc},${A1Desc},${B1Desc},${A2Desc},${B2Desc},${newCountDesc}' where PK = '${membershipEnum.PK}' and SK = '${membershipEnum.SK}' and modified_date = '${membershipEnumModifiedDate}'`;
                        txStatements.push({ "Statement": sql});

                        // body.name = "Member #" + newCountDesc;
                        // body.description = 'B Regular Membership NFT';
                        body.name = `METAFORGE Membership Card #${nextNftId}`;
                        body.description = 'Aurora Synex';

                        // // get current counter
                        // let lambdaParams = {
                        //     FunctionName: 'ch-web3',
                        //     InvocationType: 'RequestResponse', 
                        //     LogType: 'Tail',
                        //     Payload: {
                        //         action: "MEMBERSHIP_CURRENT_COUNTER",
                        //         isTest: tableName == process.env.TABLE_NAME_TEST
                        //     }
                        // };
                        // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                        // console.log("lambdaParams", lambdaParams);            
                        // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                        // console.log("lambdaResult", lambdaResult);            
                        // if(lambdaResult.Payload.errorMessage) {
                        //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                        //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                        // }            
                        // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                        // console.log("currentCounterResult", currentCounterResult);

                        // let currentCounter = newCountDesc;
                        // console.log("newCountDesc", newCountDesc);
                        // if(currentCounterResult.currentCounter != undefined) {
                        //     currentCounter = '' + currentCounterResult.currentCounter;
                        //     console.log('currentCounter', currentCounter);
                        // }

                        let _metadata = [
                            {
                                "trait_type": "Publisher",
                                "value": "Aurora Synex Co., Ltd."
                            },
                            {
                                "trait_type": "Community",
                                "value": "MetaForge"
                            },
                            {
                                "trait_type": "ID",
                                "value": nextNftId
                            },
                            {
                                "trait_type": "Rarity",
                                "value": "Common"
                            },
                            {
                                "trait_type": "Title",
                                "value": "Associate"
                            },
                            {
                                "trait_type": "Rank",
                                "value": "Bronze"
                            },
                            {
                                "trait_type": "Owner",
                                "value": member.wallet_address
                            }
                        ];

                        if(artwork.metadata) {
                            let artworkMetadata = JSON.parse(artwork.metadata);
                            if(artworkMetadata.length > 0)
                                _metadata = _metadata.concat(artworkMetadata);
                        }
                        
                        body.metadata = _metadata;

                    }
                    else {
                        console.log("All NFT for MetaForge are fully minted . METAFORGEのすべてのNFTはすべてミントされています。");
                        return {
                            Success: false,
                            Message: "METAFORGEのすべてのNFTはすべてミントされています。"
                        }
                    }
                }
                else {
                    console.log('Only member register between 6/18 and 10/31 are eligible for MetaForge NFT . 6/18から10/31の間に登録したメンバーのみがMETAFORGE NFTの対象となります。');
                    return {
                        Success: false,
                        Message: '6/18から10/31の間に登録したメンバーのみがMETAFORGE NFTの対象となります。'
                    }
                }

            }
            else {

                console.log("NFT Minting will start after 7/15 . NFTのミントは7/15以降に開始されます。");
                return {
                    Success: false,
                    Message: "NFTのミントは7/15以降に開始されます。"
                }
            }              
        }
        else if(body.nftType === 'RACINGFAN') {
            
            if(member.nft_racingfan_asset_name) {
                console.log('Member already own MEMBERSHIP NFT for project RacingFan. ' + member.userId  + " メンバーはすでにプロジェクトRacingFan.のメンバーシップNFTを所有しています。");
                return {
                    Success: false,
                    Message: 'メンバーはすでにプロジェクトRacingFan.のメンバーシップNFTを所有しています。'
                }
            }

            // if(!member.survey_completed || !member.survey_completed.includes(process.env.SURVEY_ID_RACING_FAN)) {
            //     console.log("Member not yet complete the related survey. メンバーは関連するアンケートをまだ完了していません。");
            //     return {
            //         Success: false,
            //         Message: 'メンバーは関連するアンケートをまだ完了していません。'
            //     }
            // }

            //todo : set to 2025 so that we can test minting before 2026/02/07
            // todo : set to 2030 as the end date, i.e. all user will Green. then update to gold, platinum, black based on member point
            if((today >= '2025-02-06T15:00:00.000Z' && today < '2030-02-23T14:59:00.000Z') || (body.forceRacingFan1 === true && body.appPubKey === undefined)) { // 2/7T00:00 - 2/23T23:59

                console.log("First stage NFT");

                if(rfSettings["RF1"].total !== rfSettings["RF1"].count) { 
                    
                    sql = `select * from "${tableName}" where type = 'ARTWORK' and PK = 'ARTWORK#${process.env.MEMBER_RF1_ARTWORK_ID}'`;
                    let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    artwork = artworkResult.Items.map(unmarshall)[0];
                    let imgBase64 = await getImageBase64(artwork.two_d_url);
                    body.nftURLBase64 = imgBase64;
                    body.fileName = artwork.two_d_file_name;

                    //increment counter for RF1
                    rfSettings["RF1"].count += 1;
                    let newCountDesc = `${rfSettings["RF1"].count}`;
                    
                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', enum_values = '${JSON.stringify(rfSettings)}' where PK = 'ENUM' and SK = 'RACING_FAN_NFT_SETTINGS' and modified_date = '${rfSettingsModifiedDate}'`;
                    txStatements.push({ "Statement": sql});

                    body.name = `Racing Fan Membership Card #${nextNftId}`;
                    body.description = 'Aurora Synex';

                    // // get current counter
                    // let lambdaParams = {
                    //     FunctionName: 'ch-web3',
                    //     InvocationType: 'RequestResponse', 
                    //     LogType: 'Tail',
                    //     Payload: {
                    //         action: "RACING_FAN_MEMBERSHIP_CURRENT_COUNTER",
                    //         isTest: tableName == process.env.TABLE_NAME_TEST
                    //     }
                    // };            
                    // lambdaParams.Payload = JSON.stringify(lambdaParams.Payload);            
                    // console.log("lambdaParams", lambdaParams);            
                    // const lambdaResult = await lambda.invoke(lambdaParams).promise();            
                    // console.log("lambdaResult", lambdaResult);            
                    // if(lambdaResult.Payload.errorMessage) {
                    //     console.log("lambda error message: ", JSON.stringify(lambdaResult.Payload.errorMessage));
                    //     throw new Error('Web3 Lambda error: '+ JSON.stringify(lambdaResult.Payload.errorMessage));
                    // }            
                    // let currentCounterResult = JSON.parse(lambdaResult.Payload);    
                    // console.log("currentCounterResult", currentCounterResult);

                    // let currentCounter = newCountDesc;
                    // console.log("newCountDesc", newCountDesc);
                    // if(currentCounterResult.currentCounter != undefined) {
                    //     currentCounter = '' + currentCounterResult.currentCounter;
                    //     console.log('currentCounter', currentCounter);
                    // }

                    let _metadata = [
                        {
                            "trait_type": "Community",
                            "value": "RacingFan"
                        },
                        // {
                        //     "trait_type": "ID",
                        //     "value": currentCounter
                        // },
                        {
                            "trait_type": "Rank",
                            "value": "Green"
                        },
                    ];
                    
                    body.metadata = _metadata;

                }
                else {
                    console.log(`All NFT are fully minted for first stage of Racing Fan NFT . 最初の${rfSettings["RF1"].total}枚のNFTはすべてミントされています。`);
                    return {
                        Success: false,
                        Message: `最初の${rfSettings["RF1"].total}枚のNFTはすべてミントされています。`
                    }
                }
            }
        }


        let assetId = ulid();

        // let cidNFTFile;
        let arMetadataUploadResult;
        let metadata;

        if(artwork && artworkV2 && !artworkV3) {

            body.fileName = "index.html";  // change to html so that the asset media type is html and not png

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
            
            metadata = {
                name: body.name,
                image: `https://arweave.net/${uploadResultNFTFolder.img1TxId}`,
                animation_url: `https://arweave.net/${uploadResultNFTFolder.htmlTxId}`,
                description: body.description,
                // termsOfService: process.env.TOS_URL,
                publisher: 'Aurora Synex Co., Ltd.',
                attributes: body.metadata
            }
            console.log("metadata", metadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(metadata));
            // let _metaBase64 = _metaBuffer.toString('base64')

            // console.log("_metaBase64", _metaBase64);

            // arMetadataUploadResult = await fileUpload({
            //                                         isBase64: true,
            //                                         fileData: _metaBase64,
            //                                         fileName: 'metadata.json',
            //                                         fileExtension: 'json',
            //                                         isTest: tableName == process.env.TABLE_NAME_TEST
            //                                     });

            // arMetadataUploadResult.localURL = artwork.two_d_url + ',' + artworkV2.two_d_url;

        }
        else if(artwork && artworkV2 && artworkV3) {

            body.fileName = "index.html";  // change to html so that the asset media type is html and not png

            let uploadResultNFTFolder = await folderUpload3({
                artworkIdV1: artwork.artwork_id,
                artworkIdV2: artworkV2.artwork_id,
                artworkIdV3: artworkV3.artwork_id,
                isTest: tableName == process.env.TABLE_NAME_TEST
            });

            if(uploadResultNFTFolder.errorMessage) {
                console.log('upload folder 3 err', uploadResultNFTFolder);
                return {
                    Success: false,
                    Message: 'Upload folder failed'
                }
            }
            
            metadata = {
                name: body.name,
                image: `https://arweave.net/${uploadResultNFTFolder.img1TxId}`, 
                animation_url: `https://arweave.net/${uploadResultNFTFolder.htmlTxId}`,
                description: body.description,
                //termsOfService: process.env.TOS_URL,
                publisher: 'Aurora Synex Co., Ltd.',
                attributes: body.metadata
            }
            console.log("metadata", metadata);

            // let _metaBuffer = Buffer.from(JSON.stringify(metadata));
            // let _metaBase64 = _metaBuffer.toString('base64')

            // console.log("_metaBase64", _metaBase64);

            // arMetadataUploadResult = await fileUpload({
            //                                         isBase64: true,
            //                                         fileData: _metaBase64,
            //                                         fileName: 'metadata.json',
            //                                         fileExtension: 'json',
            //                                         isTest: tableName == process.env.TABLE_NAME_TEST
            //                                     });

            // arMetadataUploadResult.localURL = artwork.two_d_url + ',' + artworkV2.two_d_url;

        }
        else if(artwork.two_d_url  && artwork.two_d_url_2 && artwork.two_d_url_3) {

            body.fileName = "index.html";  // change to html so that the asset media type is html and not png

            let uploadResultNFTFolder = await folderUpload3({
                artworkIdV1: artwork.artwork_id,
                isTest: tableName == process.env.TABLE_NAME_TEST
            });

            if(uploadResultNFTFolder.errorMessage) {
                console.log('upload folder 3 err', uploadResultNFTFolder);
                return {
                    Success: false,
                    Message: 'Upload folder failed'
                }
            }
            
            metadata = {
                name: body.name,
                image: `https://arweave.net/${uploadResultNFTFolder.img1TxId}`, 
                animation_url: `https://arweave.net/${uploadResultNFTFolder.htmlTxId}`,
                description: body.description,
                // termsOfService: process.env.TOS_URL,
                publisher: 'Aurora Synex Co., Ltd.',
                attributes: body.metadata
            }
            console.log("metadata", metadata);
        }
        else {

            if(artwork && artwork.category === 'CAR') {
                //for car, we set assetId to the artworkId of the car
                assetId = artwork.artwork_id;
            }

            let arTxId;
            
            if(artwork && artwork.ar_tx_id) {
                arTxId = artwork.ar_tx_id;
            }
            else {
                let arImageUploadResult = await fileUpload({
                    assetId: assetId,
                    isBase64: true,
                    fileData: body.nftURLBase64.split(',').pop(),
                    fileName: body.fileName,
                    fileExtension: body.fileName.split('.').pop(),
                    isTest: tableName == process.env.TABLE_NAME_TEST
                });

                arTxId = arImageUploadResult.metadata.transaction.id;

                if(artwork) {
                    sql = `update "${tableName}" set ar_tx_id = '${arTxId}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
                    //txStatements.push({ "Statement": sql});
                    let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log("updateArtworkResult", updateArtworkResult);
                }
            }

            let arVideoTxId;

            if(artwork && artwork.ar_video_tx_id) {
                console.log('using existing video artwork id');
                
                arVideoTxId = artwork.ar_video_tx_id;

                let expectedVideoFileName = `${artwork.artwork_id}_video.mp4`;
                let expectedVideoUrl = `${configs.find(x => x.key == 'S3_URL').value}/videos/${expectedVideoFileName}`;
                body.fileName = expectedVideoUrl;  // change to video so that the asset media type is mp4 and not png

                console.log("expectedVideoUrl", expectedVideoUrl);
            }
            // else if(artwork.video_url) {

            //     body.fileName = artwork.video_url;  // change to video so that the asset media type is mp4 and not png

            //     const parsedUrl = url.parse(artwork.video_url);
            //     const pathname = parsedUrl.pathname;
            //     // const decodedPathname = decodeURIComponent(pathname);
            //     const fileName = path.basename(pathname);
            //     // const videoBase64 = await getBase64FromURL(artwork.video_url);

            //     let arVideoUploadResult = await fileUpload({
            //         assetId: assetId,
            //         isBase64: false,
            //         fileData: artwork.video_url,    //videoBase64.split(',').pop(),
            //         fileName: fileName,
            //         fileExtension: artwork.video_url.split('.').pop(),
            //         isTest: tableName == process.env.TABLE_NAME_TEST,
            //         isURL: true
            //     });

            //     arVideoTxId = arVideoUploadResult.metadata.transaction.id;
            //     console.log("arVideoTxId", arVideoTxId);                

            //     if(artwork) {
            //         sql = `update "${tableName}" set ar_video_tx_id = '${arVideoTxId}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
            //         //txStatements.push({ "Statement": sql});
            //         let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            //         console.log("updateArtworkResult", updateArtworkResult);
            //     }
            // }
            else if(artwork.category === 'CAR') {

                // all car artwork are expected to have video

                let expectedVideoFileName = `${artwork.artwork_id}_video.mp4`;
                let expectedVideoUrl = `${configs.find(x => x.key == 'S3_URL').value}/videos/${expectedVideoFileName}`;

                console.log("expectedVideoUrl", expectedVideoUrl);
                
                const exists = await checkIfFileExists(expectedVideoUrl);
                if (!exists) {

                    console.log("expectedVideoUrl not exist");
                    
                    return {
                        Success: false,
                        Message: "ビデオ NFT を生成するまでお待ちください。"    //Please wait while we generating your video NFT.
                    }
                }
                
                body.fileName = expectedVideoUrl;  // change to video so that the asset media type is mp4 and not png

                let arVideoUploadResult = await fileUpload({
                    assetId: assetId,
                    isBase64: false,
                    fileData: expectedVideoUrl,
                    fileName: expectedVideoFileName,
                    fileExtension: expectedVideoUrl.split('.').pop(),
                    isTest: tableName == process.env.TABLE_NAME_TEST,
                    isURL: true
                });

                arVideoTxId = arVideoUploadResult.metadata.transaction.id;
                console.log("arVideoTxId", arVideoTxId);                

                if(artwork) {
                    sql = `update "${tableName}" set ar_video_tx_id = '${arVideoTxId}' , video_file_name = '${expectedVideoFileName}', video_url = '${expectedVideoUrl}' , modified_date = '${new Date().toISOString()}' where PK = '${artwork.PK}' and SK = '${artwork.SK}'`;
                    //txStatements.push({ "Statement": sql});
                    let updateArtworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
                    console.log("updateArtworkResult", updateArtworkResult);
                }
            }

            metadata = {
                            name: body.name,
                            image: `https://arweave.net/${arTxId}`,
                            description: body.description,
                            // termsOfService: process.env.TOS_URL,
                            publisher: 'Aurora Synex Co., Ltd.',
                            animation_url: arVideoTxId ? `https://arweave.net/${arVideoTxId}` : undefined,
                            attributes: body.metadata
                        };

            console.log("metadata", metadata);
            // let _metaBuffer = Buffer.from(JSON.stringify(metadata));
            // let _metaBase64 = _metaBuffer.toString('base64')
            // console.log("_metaBase64", _metaBase64);
            
            // arMetadataUploadResult = await fileUpload({
            //                                 assetId: assetId,
            //                                 isBase64: true,
            //                                 fileData: _metaBase64,
            //                                 fileName: 'metadata.json',
            //                                 fileExtension: 'json',
            //                                 isTest: tableName == process.env.TABLE_NAME_TEST
            //                             });

            // arMetadataUploadResult.localURL = artwork.two_d_url;
        }
        
        // console.log("arMetadataUploadResult", arMetadataUploadResult)


        sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBERWALLET#${member.wallet_address}' and type = 'ASSET' and status = 'MINTING' and store_id = '${body.storeId}'`;
        console.log("minting sql", sql);
        let mintingAssetResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(mintingAssetResult.Items.length > 0) {
            let mintingAsset = mintingAssetResult.Items.map(unmarshall)[0];
            console.log("mintingAsset", mintingAsset);
            throw new Error('Asset is already in minting status, cannot mint another asset. assetId: ' + mintingAsset.asset_id);
        }


        let mintResult;
        
        // if(nftPostResult.Success) {

            let action;
            switch(body.nftType) {
                case 'CAR':
                    action = 'MINT_CAR';
                    break;
                case 'CHARACTER':
                    action = 'MINT_CHARACTER';
                    break;
                case 'MEMBER_A':
                case 'MEMBER_B':
                    action = 'MINT_MEMBER';
                    break;
                case 'RACINGFAN':
                    action = 'MINT_RACINGFAN';
                    break;
                default:
                    throw new Error("Invalid nftType " + body.nftType);
            }
            let lambdaParams = {
                FunctionName: 'ch-web3',
                InvocationType: 'RequestResponse', 
                LogType: 'Tail',
                Payload: {
                    action: action,
                    toAddress: member.wallet_address,
                    metadata: metadata,
                    isTest: tableName == process.env.TABLE_NAME_TEST
                }
            };
            lambdaParams.Payload = JSON.stringify(lambdaParams.Payload); 
            const lambdaResult = await lambdaClient.send(new InvokeCommand(lambdaParams));
            mintResult = JSON.parse(Buffer.from(lambdaResult.Payload).toString());
            console.log("mintResult", mintResult);
            if (mintResult.errorMessage) {
                console.error("ambda error message:", JSON.stringify(mintResult.errorMessage));
                throw new Error('Lambda error: ' + JSON.stringify(mintResult.errorMessage));
            }
            if(mintResult.assetNameHex != undefined) {
                console.log("minted nft");

                if(body.nftType == "MEMBER_A" || body.nftType == "MEMBER_B") {
                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' `;
                    if(body.nftType == "MEMBER_A") {
                        sql += ` , nft_member_a_asset_name = '${mintResult.assetNameHex}' , nft_member_a_policy_id = '${mintResult.policyId}'`;
                    }
                    else if(body.nftType == "MEMBER_B") {
                        sql += ` , nft_member_b_asset_name = '${mintResult.assetNameHex}' , nft_member_b_policy_id = '${mintResult.policyId}'`;
                    }
                    
                    try {
                        const GUILD_ID = configs.find(x => x.key == 'DISCORD_GUILD_ID').value;
                        const BOT_TOKEN = configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value;
                        const DISCORD_ROLE_ID_A = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_A : process.env.DISCORD_ROLE_ID_A_TEST);
                        const DISCORD_ROLE_ID_B = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_B : process.env.DISCORD_ROLE_ID_B_TEST);
                        
                        // grant proj A discord role
                        if(body.nftType === 'MEMBER_A' && member.discord_user_id && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('LITTLEBLUE'))) {

                            let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${DISCORD_ROLE_ID_A}`
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
                            console.log("grant discord role for project A result", grantRoleResult);

                            // // grant bronze ranking role
                            // let discordRoleId = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_BRONZE_LITTLEBLUE : process.env.DISCORD_ROLE_ID_BRONZE_LITTLEBLUE_TEST);
                            // let discordRoleName = "LITTLEBLUE_BRONZE";

                            // if(member.discord_roles === undefined || !member.discord_roles.split(',').includes('BRONZE')) {
                            //     let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${discordRoleId}`
                            //     console.log('grant discord role for bronze url', url);
                            //     let _headers = {
                            //                         "Authorization": `Bot ${BOT_TOKEN}`,
                            //                         "Content-Type": "application/json"
                            //                     };
                            //     let grantRoleResult = await axios.put(url,
                            //                                         null,
                            //                                         {
                            //                                             headers: _headers,
                            //                                         });
                            //     console.log("grant discord role for bronze ranking result", grantRoleResult);

                            //     sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',LITTLEBLUE,' + discordRoleName : 'LITTLEBLUE,' + discordRoleName}' `;
                            // }
                            // else {
                            //     sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',LITTLEBLUE' : 'LITTLEBLUE'}' `;
                            // }

                            sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',LITTLEBLUE' : 'LITTLEBLUE'}' `;

                        }

                        // grant proj B discord role
                        if(body.nftType === 'MEMBER_B' && member.discord_user_id && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('METAFORGE'))) {
                            let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${DISCORD_ROLE_ID_B}`
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
                            console.log("grant discord role for project B result", grantRoleResult);

                            // // grant bronze ranking role
                            // let discordRoleId = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_BRONZE_METAFORGE : process.env.DISCORD_ROLE_ID_BRONZE_METAFORGE_TEST);
                            // let discordRoleName = "METAFORGE_BRONZE";

                            // if(member.discord_roles === undefined || !member.discord_roles.split(',').includes('BRONZE')) {
                            //     let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${discordRoleId}`
                            //     console.log('grant discord role for bronze url', url);
                            //     let _headers = {
                            //                         "Authorization": `Bot ${BOT_TOKEN}`,
                            //                         "Content-Type": "application/json"
                            //                     };
                            //     let grantRoleResult = await axios.put(url,
                            //                                         null,
                            //                                         {
                            //                                             headers: _headers,
                            //                                         });
                            //     console.log("grant discord role for bronze ranking result", grantRoleResult);

                            //     sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',METAFORGE,' + discordRoleName : 'METAFORGE,' + discordRoleName}' `;
                            // }
                            // else {
                            //     sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',METAFORGE' : 'METAFORGE'}' `;
                            // }

                            sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',METAFORGE' : 'METAFORGE'}' `;
                            
                        }
                    } catch (err) {
                        console.log(err);
                        const _message = {
                            Subject: 'Honda Cardano Error - ch-nft-mint-post',
                            Message: "unable to grant discord role for discord user id " + member.discord_user_id + ' for nftType ' + body.nftType,
                            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                        };
                        await sns.publish(_message).promise();
                    }

                    sql += ` where PK = '${member.PK}' and SK = '${member.SK}'`;

                    txStatements.push({ "Statement": sql});
                    

                    
                    //let updateMemberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                    //console.log("updateMemberResult", updateMemberResult);
                }
                
                // grant RACINGFAN discord role
                if(body.nftType === 'RACINGFAN' ) {

                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' `;
                    sql += ` , nft_racingfan_asset_name = '${mintResult.assetNameHex}' , nft_racingfan_policy_id = '${mintResult.policyId}'`;

                    if(member.discord_user_id && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('RACINGFAN'))) {
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
                            console.log("grant discord role for FacingFan result", grantRoleResult);
                            
                        } catch (err) {
                            console.log(err);
                            const _message = {
                                Subject: 'Honda Cardano Error - ch-nft-mint-post',
                                Message: "unable to grant discord role for discord user id " + member.discord_user_id + ' for nftType ' + body.nftType,
                                TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
                            };
                            await sns.publish(_message).promise();
                        }
                        sql += ` , discord_roles = '${member.discord_roles ? member.discord_roles + ',RACINGFAN' : 'RACINGFAN'}' `;
                    }
                    
                    sql += ` where PK = '${member.PK}' and SK = '${member.SK}'`;

                    txStatements.push({ "Statement": sql});
                    
                }

                const statements = { "TransactStatements": txStatements };  
                console.log("statements", JSON.stringify(statements));
                
                const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
                console.log("update for membership dbResult", dbTxResult);
                
            }
            else {
                console.log('Failed to mint NFT in blockchain . ブロックチェーンでのNFTの鋳造に失敗しました');
                return {
                    Success: false,
                    Message: "NFTをMintする際に、ブロックチェーン（ポリゴン）で、一時的な問題が発生している可能性があります。システム管理者側で、エラーを確認し次第、解消致します。解消には時間がかかる可能性がありますので、ブラウザを閉じて、お待ちください。もし、1日経っても、解消しない場合には、Discordの質問チャネルからご連絡をいただけますと幸いです"
                }
            }

            
        // }            
        // else {
        //     console.log("Failed to post NFT", nftPostResult.Message);
        //     throw new Error(nftPostResult.Message);
        //     // return {
        //     //     Success: false,
        //     //     Message: nftPostResult.Message
        //     // }
        // }

        console.log("mintResult", mintResult);        
            if(mintResult.transactionHash) {
                console.log("minted nft");

            let nftPostResult = await nftPost({
                                    passcode: configs.find(x => x.key == 'PASSCODE').value,
                                    assetId: assetId,
                                    memberId: body.memberId ? body.memberId : member.user_id,
                                    hiddenFile: metadata.image,
                                    nftFile: metadata.animation_url ? metadata.animation_url : metadata.image,    //"ipfs://" + cidNFTFile.metadata.ipnft,
                                    previewImageFile: metadata.image,
                                    // metadataFile: `https://arweave.net/${arMetadataUploadResult.metadata.transaction.id}`,
                                    metadata: metadata,
                                    name: body.name,
                                    description: body.description,
                                    storeId: body.storeId,
                                    category: body.category,
                                    subCategory: body.subCategory,
                                    tags: [body.nftType],
                                    royalty: body.royalty,
                                    licenseId: body.licenseId,
                                    fileExtension: body.fileName.split('.').pop(),
                                    nftType: body.nftType,
                                    // localURL: arMetadataUploadResult.localURL
                                    unit: mintResult.unit,  // similar to contract address + token id
                                    policyId: mintResult.policyId,  // similar to contract address
                                    assetName: mintResult.assetNameHex,   // similar to token id
                                }, headers['origin']);

            console.log("nft post result", nftPostResult);

            if(body.queueId) {
                let nftResult = {
                    assetName: mintResult.assetNameHex,
                    policyId: mintResult.policyId,
                    assetId: assetId,
                    transactionHash: mintResult.transactionHash
                }
                sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' , result = '${JSON.stringify(nftResult)}' where PK = 'QUEUE#MINT#${body.queueId}' and SK = '${member.SK}'`;
                console.log("sql", sql);
                let updateQueueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("updateQueueResult", updateQueueResult);
            }
            
            return {
                Success: true,
                Data: {
                    assetName: mintResult.assetNameHex,
                    policyId: mintResult.policyId,
                    unit: mintResult.unit,
                    assetId: assetId
                }
            }
        }
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-nft-mint-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-mint-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-mint-post  ${e.message}\n\nStack trace:\n${e.stack}`,
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