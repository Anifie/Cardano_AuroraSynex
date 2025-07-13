import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

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

        if(token) {
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
            //     memberResult = await db.executeStatement({Statement: sql}).promise();
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


        if(!member.role?.includes('ADMIN')) {
            if(member.user_id != artwork.user_id) {
                return {
                    Success: false,
                    Message: 'This artwork does not belong to this user'
                }
            }    
        }

        let txStatements = [];
        
        console.log('deleting artwork');
        
        let deleteSQL = `DELETE FROM "${tableName}" WHERE PK = '${artwork.PK}' AND SK = '${artwork.SK}'`;
        txStatements.push({ "Statement": deleteSQL});

        if(artwork.artwork_type == 'COMPONENT') {
            let result = await dbClient.send(new ExecuteStatementCommand({
                Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_EN#${artwork.name_en}'`
            }));

            let componentEnum = result.Items.map(unmarshall)[0];
            let enumValuesArr = componentEnum.enum_values.split(',');
            let enumDescsArr = componentEnum.enum_description.split(',');
            let enumValuesIndex = enumValuesArr.indexOf(artwork.value_en);
            let enumDescIndex = enumDescsArr.indexOf(artwork.artwork_id);
            if(enumValuesIndex >= 0) {
                let enumValuesNew = enumValuesArr.splice(enumValuesIndex, 1).join(); // remove element by value
                let enumDescsNew = enumDescsArr.splice(enumDescIndex, 1).join(); // remove element by value
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


            if(artwork.value_jp) {
                result = await await dbClient.send(new ExecuteStatementCommand({
                    Statement: `SELECT * FROM "${tableName}" WHERE PK = 'ENUM' and SK = 'COMPONENT_JP#${artwork.name_jp}'`
                }));
    
                componentEnum = result.Items.map(unmarshall)[0];
                enumValuesArr = componentEnum.enum_values.split(',');
                enumDescsArr = componentEnum.enum_description.split(',');
                enumValuesIndex = enumValuesArr.indexOf(artwork.value_jp);
                enumDescIndex = enumDescsArr.indexOf(artwork.artwork_id);
                if(enumValuesIndex >= 0) {
                    let enumValuesNew = enumValuesArr.splice(enumValuesIndex, 1).join(); // remove element by value
                    let enumDescsNew = enumDescsArr.splice(enumDescIndex, 1).join(); // remove element by value
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
        
        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        
        const dbTxResult = await await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("delete artwork dbResult", dbTxResult);

        return {
            Success: true
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-artwork-delete ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-artwork-delete - ' + random10DigitNumber,
            Message: `Error in ch-artwork-delete: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key=='SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await sns.publish(message).promise();
        
        const response = {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};