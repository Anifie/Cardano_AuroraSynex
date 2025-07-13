const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const sns = new AWS.SNS();
var jwt = require('jsonwebtoken');
const axios = require("axios");

export const handler = async (event) => {
    console.log("event", event);
    
    let tableName;

    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);


        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);
    
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
        
        let memberId;
        
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

        let member = memberResult.Items.map(unmarshall)[0];

        if(member.role !== 'ADMIN' && member.role !== 'OPERATOR') {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }

        if(!body.questionId) {
            return {
                Success: false,
                Message: "questionId is required"
            };
        }

        sql = `select * from "${tableName}" where PK = 'VOTE_DISCORD_QUESTION#${body.questionId}' and SK = 'VOTE_DISCORD_QUESTION'`;
        let questionResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(questionResult.Items.length == 0) {
            return {
                Success: false,
                Message: "Vote question not found. Question Id: " + body.questionId
            }
        }

        
        let voteQuestion;

        sql = `select * from "${tableName}" where PK = 'VOTE_DISCORD_QUESTION#${body.questionId}'`;
        let questionAndAnswerResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        let questionAndAnswers = questionAndAnswerResult.Items.map(unmarshall);
        for (let i = 0; i < questionAndAnswers.length; i++) {
            const obj = questionAndAnswers[i];

            if(obj.type === 'VOTE_DISCORD_QUESTION') {
                voteQuestion = obj;
            }

            sql = `delete from "${tableName}" where PK = '${obj.PK}' and SK = '${obj.SK}'`;
            let deleteResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            console.log("deleteResult", deleteResult);
        }

        if(voteQuestion && voteQuestion.is_posted_to_discord === true) {
            let isTest = tableName === process.env.TABLE_NAME_TEST;
            const BOT_TOKEN = (isTest ? process.env.DISCORD_BOT_TOKEN_TEST : process.env.DISCORD_BOT_TOKEN);
            // const CHANNEL_ID = (isTest ? process.env.CHANNEL_ID_VOTE_TEST : process.env.CHANNEL_ID_VOTE);
            try {
                let response = await axios.delete(`https://discord.com/api/v10/channels/${voteQuestion.discord_channel_id}/messages/${voteQuestion.discord_message_id}`, {
                    headers: {
                        'Authorization': `Bot ${BOT_TOKEN}`,
                        'Content-Type': 'application/json'
                    }
                })    
                console.log(response.data);
                console.log('delete discordMessage jsonResult', response.data);
            } catch (_err) {
                console.log("failed to delete discord message", _err);    
            }
        }
        
        return {
            Success: true
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-vote-discord-question-delete ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-vote-discord-question-delete - ' + random10DigitNumber,
            Message: `Error in ch-vote-discord-question-delete: ${e.message}\n\nStack trace:\n${e.stack}`,
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