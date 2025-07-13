import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import axios from 'axios';
import * as jose from 'jose';
import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

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

        if(body.surveyId === undefined) {
            return {
                Success: false,
                Message: 'surveyId is required'
            };
        }

        if(body.answers === undefined) {
            return {
                Success: false,
                Message: 'answers is required'
            };
        }

        // body.answers = JSON.parse(body.answers);

        for (let i = 0; i < body.answers.length; i++) {
            const answer = body.answers[i];
            
            if(answer.questionIndex === undefined) {
                return {
                    Success: false,
                    Message: 'questionIndex is required'
                };
            }
    
            if(answer.offeredAnswerIndex === undefined && answer.offeredAnswerText === undefined) {
                return {
                    Success: false,
                    Message: 'either offeredAnswerIndex or offeredAnswerText is required'
                };
            }
        }

        let member;

        if(body.appPubKey){
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
        else if (body.uuid && body.token) {
            
            
            const _response = await axios.post(process.env.PARTICLE_API_URL,
                {
                    jsonrpc: "2.0",
                    id: 0,
                    method: "getUserInfo",
                    params: [body.uuid, body.token],
                },
                {
                    auth: {
                        username: process.env.PARTICLE_PROJECT_ID,
                        password: process.env.PARTICLE_SERVER_KEY,
                    },
                }
            );

            console.log(_response.data);

            if(_response.data.error) {
                console.log("Error verify user info", _response.data.error);
                throw new Error(JSON.stringify(_response.data.error))
            }

            let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + body.uuid }],}));
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        else if(body.discordId) {
            let sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE discord_user_id = '${body.discordId.toUpperCase()}' and type = 'MEMBER'`;
            let memberResult = await fetchAllRecords(sql);
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.map(unmarshall)[0];

            if(member.survey_completed && member.survey_completed.includes(body.surveyId)) {
                return {
                    Success: false,
                    Message: 'このアンケートにはすでに回答しています。ありがとうございました。'   // Member already responded to this survey
                };
            }

        }
        else {
            return {
                Success: false,
                Message: "Missing login info"
            }
        }

        

        
        // if(body.walletAddress != member.wallet_address && body.walletAddress != member.wallet_address_smartaccount) {
        //     return {
        //         Success: false,
        //         Message: 'Invalid wallet address',
        //     };
        // }

        let sql = `select * from "${tableName}"."ByTypeCreatedDate" where PK = 'SURVEY#${body.surveyId}' and SK = 'SURVEY' and type = 'SURVEY'`;
            
        let surveyResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(surveyResult.Items.length == 0) {
            console.log("survey not found: " + body.surveyId);
            const response = {
                Success: false,
                Message: "survey not found: " + body.surveyId
            };
        }

        let survey = surveyResult.Items.map(unmarshall)[0];
        
        // if(survey.is_open == false) {
        //     return {
        //         Success: false,
        //         Message: 'survey is closed',
        //     }
        // }

        sql = `select * from "${tableName}"."ByTypeCreatedDate"  where type = 'SURVEY_QUESTION' and PK = 'SURVEY#${body.surveyId}'`;
        console.log("sql", sql);
        let surveyQuestionsResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("surveyQuestionsResult", JSON.stringify(surveyQuestionsResult));

        sql = `select * from "${tableName}" where PK = '${member.PK}' and begins_with("SK", 'SURVEY#${body.surveyId}#QUESTION#')`;
        console.log("sql", sql);
        let surveyAnsTotalResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("surveyAnsTotalResult", JSON.stringify(surveyAnsTotalResult));

        console.log("surveyAnsTotalResult.Items.length", surveyAnsTotalResult.Items.length);
        console.log("surveyQuestionsResult.Items.length", surveyQuestionsResult.Items.length);
        
        let txStatements = [];

        let sqlUpdateMember;

        for (let i = 0; i < body.answers.length; i++) {
            const answer = body.answers[i];

            sql = `select * from "${tableName}" where PK = '${member.PK}' and SK = 'SURVEY#${body.surveyId}#QUESTION#${answer.questionIndex}'`;
            let surveyAnsResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(surveyAnsResult.Items.length > 0) {
                console.log("Updating.. Member already answered the survey question");
                // return {
                //     Success: false,
                //     Message: "Member already answered the survey question"
                // };

                let surveyAns = surveyAnsResult.Items.map(unmarshall)[0];

                sql = `UPDATE "${tableName}" set modified_date = '${new Date().toISOString()}' , offered_answer_index = '${answer.offeredAnswerIndex ? answer.offeredAnswerIndex : ''}' , offered_answer_text = '${answer.offeredAnswerText ? answer.offeredAnswerText : ''}' WHERE PK = '${surveyAns.PK}' and SK = '${surveyAns.SK}'`;
                //console.log("sql", sql);
                txStatements.push({ "Statement": sql });   

            }
            else {

                // update member campaign code
                if((body.surveyId == process.env.CAMPAIGN_CODE_SURVEY_ID_A ||body.surveyId == process.env.CAMPAIGN_CODE_SURVEY_ID_B)
                    && answer.questionIndex == process.env.CAMPAIGN_CODE_QUESTION_INDEX 
                    && answer.offeredAnswerText != '' 
                    && answer.offeredAnswerText != undefined) {

                        // validate campaign code
                        sql = `SELECT * FROM "${tableName}" WHERE PK = 'CAMPAIGNCODE'`;
                        let campaignCodeResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                        let campaignCodes = campaignCodeResult.Items.map(unmarshall);
                        let foundCampaignCode = campaignCodes.find(x => x.code == answer.offeredAnswerText);
                        if(!foundCampaignCode) {
                            console.log("Invalid campaign code " + answer.offeredAnswerText + " . 無効なキャンペーンコードです");
                            return {
                                Success: false,
                                Message: '無効なキャンペーンコードです ' + answer.offeredAnswerText
                            }
                        }
                        
                        if(foundCampaignCode.is_used == true) {
                            console.log("Campaign code is already been used " + answer.offeredAnswerText + " キャンペーンコードはすでに使用されています");
                            return {
                                Success: false,
                                Message: 'キャンペーンコードはすでに使用されています ' + answer.offeredAnswerText
                            }
                        }

                        // update member campagin code
                        if(!member.campaign_code) {
                            let project = (body.surveyId == process.env.CAMPAIGN_CODE_SURVEY_ID_A ? 'PALEBLUEDOT' : 'METAGARAGE');

                            sqlUpdateMember = ` campaign_code = '${answer.offeredAnswerText}', campaign_code_project = '${project}' `;
                            // txStatements.push({ "Statement": sql });
                            // let updateMemberCampaignCodeResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                            // console.log("updateMemberCampaignCodeResult", updateMemberCampaignCodeResult);

                            sql = `update "${tableName}" set is_used = true , modified_date = '${new Date().toISOString()}' , project = '${project}' where PK = '${foundCampaignCode.PK}' and SK = '${foundCampaignCode.SK}'`;
                            txStatements.push({ "Statement": sql });
                            // let updateCampaignCodeResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                            // console.log("updateCampaignCodeResult", updateCampaignCodeResult);
                        }
                        else {
                            if(member.campaign_code != answer.offeredAnswerText) {
                                console.log("Each member only can have 1 campaign code");
                                return {
                                    Success: false,
                                    Message: 'キャンペーンコードは会員1名につき1つまでとなります'
                                }
                            }
                        }
                }

                sql = `INSERT INTO "${tableName}" 
                        VALUE { 
                        'PK': '${member.PK}', 
                        'SK': 'SURVEY#${body.surveyId}#QUESTION#${answer.questionIndex}', 
                        'type': 'SURVEY_ANSWER', 
                        'user_id': '${member.user_id}', 
                        'wallet_address': '${member.wallet_address}', 
                        'discord_user_id': '${member.discord_user_id}', 
                        'survey_id': '${body.surveyId}', 
                        'question_index': '${answer.questionIndex}', 
                        'offered_answer_index': '${answer.offeredAnswerIndex ? answer.offeredAnswerIndex : ''}',
                        'offered_answer_text': '${answer.offeredAnswerText ? answer.offeredAnswerText : ''}',
                        'created_date': '${new Date().toISOString()}'}`;
                // let insertAnswerResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                // console.log("insertAnswerResult", insertAnswerResult);
                txStatements.push({ "Statement": sql });   
            } 
        }

        

        // grant preregister role (Pre-participation role) after finish survey
        if(body.surveyId == process.env.SURVEY_ID_REGISTER && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('PREREGISTER'))) {

            const GUILD_ID = configs.find(x => x.key == 'DISCORD_GUILD_ID').value;
            const BOT_TOKEN = configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value;
            const DISCORD_ROLE_ID_PREREGISTER = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_PREREGISTER : process.env.DISCORD_ROLE_ID_PREREGISTER_TEST);

            // grant discord pregister role
            let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${DISCORD_ROLE_ID_PREREGISTER}`
            console.log('grant discord pregister role url', url);
            let _headers = {
                                "Authorization": `Bot ${BOT_TOKEN}`,
                                "Content-Type": "application/json"
                            }

            let grantRoleResult = await axios.put(url,
                                                null,
                                                {
                                                    headers: _headers,
                                                });

            console.log("grant discord preregister role result", grantRoleResult);

            if(grantRoleResult.status != 204 && grantRoleResult.status != 200) {
                console.log('Error granting Discord role. Discord ロールの付与エラー');
                return {
                    Sucess: false,
                    Message: 'Discord ロールの付与エラー'
                }
            }

            // discord_member = '${JSON.stringify(discordData.member)}' ,
            if(sqlUpdateMember)
                sqlUpdateMember += ` , discord_user_id = '${member.discord_user_id}',  discord_roles = '${member.discord_roles ? member.discord_roles + ',PREREGISTER' : 'PREREGISTER'}' `;
            else
                sqlUpdateMember = ` discord_user_id = '${member.discord_user_id}',  discord_roles = '${member.discord_roles ? member.discord_roles + ',PREREGISTER' : 'PREREGISTER'}' `;

            txStatements.push({ "Statement": sql });
            // let updateDiscordRoleResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // console.log("updateDiscordRoleResult", updateDiscordRoleResult);
        }

        // grant survey participant role after finish survey
        if(body.surveyId == process.env.SURVEY_ID_PROJECT_END && (member.discord_roles === undefined || !member.discord_roles.split(',').includes('PROJECT_END_SURVEY_PARTICIPANT'))) {

            const GUILD_ID = configs.find(x => x.key == 'DISCORD_GUILD_ID').value;
            const BOT_TOKEN = configs.find(x => x.key == 'DISCORD_BOT_TOKEN').value;
            const DISCORD_ROLE_ID_PROJECT_END_SURVEY_PARTICIPANT = (tableName == process.env.TABLE_NAME ? process.env.DISCORD_ROLE_ID_PROJECT_END_SURVEY_PARTICIPANT : process.env.DISCORD_ROLE_ID_PROJECT_END_SURVEY_PARTICIPANT_TEST);

            // grant discord pregister role
            let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id}/roles/${DISCORD_ROLE_ID_PROJECT_END_SURVEY_PARTICIPANT}`
            console.log('grant discord pregister role url', url);
            let _headers = {
                                "Authorization": `Bot ${BOT_TOKEN}`,
                                "Content-Type": "application/json"
                            }

            let grantRoleResult = await axios.put(url,
                                                null,
                                                {
                                                    headers: _headers,
                                                });

            console.log("grant discord project end role result", grantRoleResult);

            if(grantRoleResult.status != 204 && grantRoleResult.status != 200) {
                console.log('Error granting Discord role. Discord ロールの付与エラー');
                return {
                    Sucess: false,
                    Message: 'Discord ロールの付与エラー'
                }
            }

            // discord_member = '${JSON.stringify(discordData.member)}' ,
            if(sqlUpdateMember)
                sqlUpdateMember += ` , discord_user_id = '${member.discord_user_id}',  discord_roles = '${member.discord_roles ? member.discord_roles + ',PROJECT_END_SURVEY_PARTICIPANT' : 'PROJECT_END_SURVEY_PARTICIPANT'}' `;
            else
                sqlUpdateMember = ` discord_user_id = '${member.discord_user_id}',  discord_roles = '${member.discord_roles ? member.discord_roles + ',PROJECT_END_SURVEY_PARTICIPANT' : 'PROJECT_END_SURVEY_PARTICIPANT'}' `;

            // txStatements.push({ "Statement": sql });
            // let updateDiscordRoleResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // console.log("updateDiscordRoleResult", updateDiscordRoleResult);
        }

        if(member.survey_completed) {
            let surveysCompleted = member.survey_completed.split(',');
            if (!surveysCompleted.includes(body.surveyId)) {
                
                if(sqlUpdateMember)
                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', survey_completed = '${member.survey_completed},${body.surveyId}' , ${sqlUpdateMember} where PK = '${member.PK}' and SK = '${member.SK}'`;
                else
                    sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', survey_completed = '${member.survey_completed},${body.surveyId}' where PK = '${member.PK}' and SK = '${member.SK}'`;

                txStatements.push({ "Statement": sql });
                // let updateMemberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                // console.log("updateMemberResult", updateMemberResult);

                sql = `update "${tableName}" set completed = '${(survey.completed ? parseInt(survey.completed) : 0) + 1}' where PK = '${survey.PK}' and SK = '${survey.SK}'`;
                txStatements.push({ "Statement": sql });
                // let updateSurveyResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                // console.log("updateSurveyResult", updateSurveyResult);
            }
        }
        else  {
            if(sqlUpdateMember)
                sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', survey_completed = '${body.surveyId}' , ${sqlUpdateMember} where PK = '${member.PK}' and SK = '${member.SK}'`;
            else
                sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}', survey_completed = '${body.surveyId}' where PK = '${member.PK}' and SK = '${member.SK}'`;

            txStatements.push({ "Statement": sql });
            // let updateMemberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // console.log("updateMemberResult", updateMemberResult);

            sql = `update "${tableName}" set completed = '${(survey.completed ? parseInt(survey.completed) : 0) + 1}' where PK = '${survey.PK}' and SK = '${survey.SK}'`;
            txStatements.push({ "Statement": sql });
            // let updateSurveyResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            // console.log("updateSurveyResult", updateSurveyResult);
        }

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("insert survey answer", dbTxResult);

        return {
                    Success: true
                };
        
    } catch (e) {

        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-survey-answer-all-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-survey-answer-all-post - ' + random10DigitNumber,
            Message: `Error in ch-survey-answer-all-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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