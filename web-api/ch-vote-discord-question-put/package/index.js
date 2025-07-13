const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const axios = require("axios");
const sns = new AWS.SNS();
var jwt = require('jsonwebtoken');

const discordMessage = async (messagePayload, channelId, isTest) => {
    console.log("discordMessage", messagePayload, channelId, isTest);

    const BOT_TOKEN = (isTest ? process.env.DISCORD_BOT_TOKEN_TEST : process.env.DISCORD_BOT_TOKEN);
    // const CHANNEL_ID = (isTest ? process.env.CHANNEL_ID_VOTE_TEST : process.env.CHANNEL_ID_VOTE);

    let response = await axios.post(`https://discord.com/api/v10/channels/${channelId}/messages`, messagePayload, {
        headers: {
            'Authorization': `Bot ${BOT_TOKEN}`,
            'Content-Type': 'application/json'
        }
    })

    console.log(response.data);
    console.log('discordMessage jsonResult', response.data);
    return response.data;
}

const removeDiscordMessage = async (messageId, channelId, isTest) => {
    console.log("removeDiscordMessage", messageId, channelId, isTest);

    const BOT_TOKEN = (isTest ? process.env.DISCORD_BOT_TOKEN_TEST : process.env.DISCORD_BOT_TOKEN);
    // const CHANNEL_ID = (isTest ? process.env.CHANNEL_ID_VOTE_TEST : process.env.CHANNEL_ID_VOTE);

    let response = await axios.delete(`https://discord.com/api/v10/channels/${channelId}/messages/${messageId}`, {
        headers: {
            'Authorization': `Bot ${BOT_TOKEN}`,
            'Content-Type': 'application/json'
        }
    })

    console.log(response.data);
    console.log('removeDiscordMessage jsonResult', response.data);
    return response.data;
}

const validateChannelId = async (channelId, isTest) => {
    console.log("validateChannelId", channelId, isTest);

    const BOT_TOKEN = (isTest ? process.env.DISCORD_BOT_TOKEN_TEST : process.env.DISCORD_BOT_TOKEN);
    // const CHANNEL_ID = (isTest ? process.env.CHANNEL_ID_VOTE_TEST : process.env.CHANNEL_ID_VOTE);

    try {
        let response = await axios.get(`https://discord.com/api/v10/channels/${channelId}`, {
            headers: {
                'Authorization': `Bot ${BOT_TOKEN}`,
                'Content-Type': 'application/json'
            }
        })
    
        console.log(response.data);
        console.log('validateChannelId jsonResult', response.data);
        
        return true;

    } catch (error) {
        
        console.log("validateChannelId", error);

        if (error.response && error.response.status === 404) {
            return false; // Channel ID is invalid
        } else {
            throw new Error(error)
        }
    }
}

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

        let question = questionResult.Items.map(unmarshall)[0];

        if(body.discordChannelId) {
            let validateResult = await validateChannelId(body.discordChannelId, tableName === process.env.TABLE_NAME_TEST);
            if(validateResult === false) {
                console.log("Invalid Discord channel id " + body.discordChannelId);
                return {
                    Success: false,
                    Message: "Invalid Discord channel id " + body.discordChannelId
                }
            }
        }

        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}' `;

        if(body.title) {
            sql += ` , title = '${body.title}'`;
        }

        if(body.description) {
            sql += ` , description = '${body.description}'`;
        }

        if(body.projectName) {
            sql += ` , project_name = '${body.projectName}'`;
        }

        if(body.discordChannelId) {
            sql += ` , discord_channel_id = '${body.discordChannelId}'`;
        }

        if(body.isOpen !== undefined) {
            sql += ` , is_open = ${body.isOpen}`;
        }

        if(body.isMultiSelect !== undefined) {
            sql += ` , is_multi_select = ${body.isMultiSelect}`;
        }

        if(body.multiSelectLabel !== undefined) {
            sql += ` , multi_select_label = '${body.multiSelectLabel}' `;
        }

        let dbParams = [];

        if(body.choices) {
            let jsonChoices = JSON.stringify(body.choices);
            if(jsonChoices != question.choices) {
                sql += ` , choices = ?`;
                dbParams.push({S: JSON.stringify(body.choices)});
            }
        }

        if(body.sendToDiscordImmediately === true) {
            let messagePayload = {
                embeds: [
                    {
                        title: body.title || question.title,
                        description: body.description,
                        // color: 3447003, // Decimal color code
                        // fields: [
                        //     {
                        //         name: 'Field Name',
                        //         value: 'Field Value',
                        //         inline: true
                        //     }
                        // ]
                    }
                ],
                components: []
            };

            if(body.isMultiSelect) {

                if(!body.multiSelectLabel) {
                    return {
                        Success: false,
                        Message: 'multiSelectLabel is required'
                    }
                }
    
                let foundMissingLabelChoice = body.choices.find(x=>x.label === undefined || x.label === '');
                if(foundMissingLabelChoice) {
                    return {
                        Success: false,
                        Message: 'found choice with missing label. label is required for every choice'
                    }
                }
    
                let _component = {
                    type: 3,    // Type 3 for select menu
                    custom_id: `VOTE#${body.questionId}#`,
                    placeholder: body.multiSelectLabel,
                    min_values: 1,
                    max_values: body.choices.length,
                    options: body.choices.map((choice, index) => ({label: (choice.label ? choice.label : undefined), emoji: (choice.emoji ? { name: choice.emoji } : undefined), value: `VOTE#${body.questionId}#${index}` }))
                };
    
                messagePayload.components.push({
                    type: 1, // ActionRow
                    components: [_component]
                });
    
                console.log("messagePayload", JSON.stringify(messagePayload));
            }
            else {
                let chunkSize = 5;
                for (let i = 0; i < body.choices.length; i += chunkSize) {
                    
                    const chunk = body.choices.slice(i, i + chunkSize);
    
                    let _rowComponents = [];
    
                    for (let j = 0; j < chunk.length; j++) {
                        const choice = chunk[j];
    
                        let _component = {
                            type: 2, // Button
                            style: 2, // Primary style
                            custom_id: `VOTE#${body.questionId}#` + (i+j)
                        };
            
                        if(choice.label) {
                            _component.label = choice.label;
                        }
                        
                        if(choice.emoji) {
                            _component.emoji = {
                                name: choice.emoji
                            };
                        }
    
                        _rowComponents.push(_component);
                    }
                    
                    messagePayload.components.push({
                        type: 1, // ActionRow
                        components: _rowComponents
                    });
    
                    console.log("messagePayload", JSON.stringify(messagePayload));
    
                }
            }

            if(question.is_posted_to_discord === true && question.discord_message_id) {
                try {
                    let response = await removeDiscordMessage(question.discord_message_id, question.discord_channel_id, tableName == process.env.TABLE_NAME_TEST);
                    console.log("delete resp", response);   
                } catch (_err) {
                    console.log("Failed to delete messaga", _err);
                }
            }

            let _discordChannelId;
            if(body.discordChannelId)
                _discordChannelId = body.discordChannelId;
            else 
                _discordChannelId = question.discord_channel_id;
            
            let messageResult = await discordMessage(messagePayload, _discordChannelId, tableName == process.env.TABLE_NAME_TEST)    
            console.log("discordMessageResult", messageResult);

            let discordMessageId = messageResult.id;
            sql += ` , discord_message_id = '${discordMessageId}' , is_posted_to_discord = true `;
        }

        sql += ` where PK = '${question.PK}' and SK = '${question.SK}'`;

        let statement;

        if(dbParams.length > 0) {
            statement = {Statement: sql, Parameters: dbParams};
        }
        else {
            statement = {Statement: sql};
        }
                        
        console.log(statement);
        let dbResult = await db.executeStatement(statement).promise();
        console.log("update vote question dbResult", dbResult);
        
        return {
            Success: true
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-vote-discord-question-put ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-vote-discord-question-put - ' + random10DigitNumber,
            Message: `Error in ch-vote-discord-question-put: ${e.message}\n\nStack trace:\n${e.stack}`,
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