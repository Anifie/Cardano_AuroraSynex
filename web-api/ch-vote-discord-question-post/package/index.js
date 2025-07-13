const AWS = require('aws-sdk');
const ULID = require('ulid');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const jose = require("jose");
const md5 = require("md5");
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

        if(!body.description) {
            return {
                Success: false,
                Message: "description is required"
            };
        }

        if(body.sendToDiscordImmediately === undefined) {
            return {
                Success: true,
                Message: 'sendToDiscordImmediately is required'
            }
        }

        if(body.choices === undefined) {
            return {
                Success: true,
                Message: 'choices is required'
            }
        }

        if(body.discordChannelId === undefined) {
            return {
                Success: true,
                Message: 'discordChannelId is required'
            }
        }

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

        const questionId = ULID.ulid();

        let messagePayload = {
            embeds: [
                {
                    title: body.title,
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

        if(body.artwork_ids) {
            let artworkId = body.artworkIds.split(',')[0];
            let sql = `select * from "${tableName}" where PK = 'ARTWORK#${artworkId}' and type = 'ARTWORK'`;
            console.log("sql", sql);
            let artworkResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            let artwork = artworkResult.Items.map(unmarshall)[0];
            messagePayload.embeds[0].image.url = artwork.two_d_url;
        }

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
                custom_id: `VOTE#${questionId}#`,
                placeholder: body.multiSelectLabel,
                min_values: 1,
                max_values: body.choices.length,
                options: body.choices.map((choice, index) => ({label: (choice.label ? choice.label : undefined), emoji: (choice.emoji ? { name: choice.emoji } : undefined), value: `VOTE#${questionId}#${index}` }))
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
                        custom_id: `VOTE#${questionId}#` + (i+j)
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
        
        let discordMessageId;

        if(body.sendToDiscordImmediately === true) {
            let messageResult = await discordMessage(messagePayload, body.discordChannelId, tableName == process.env.TABLE_NAME_TEST);
            console.log("discordMessageResult", JSON.stringify(messageResult));

            discordMessageId = messageResult.id;
        }

        console.log('inserting new vote discord question');
        sql = `INSERT INTO "${tableName}" 
                VALUE { 
                'PK': 'VOTE_DISCORD_QUESTION#${questionId}', 
                'SK': 'VOTE_DISCORD_QUESTION', 
                'type': 'VOTE_DISCORD_QUESTION', 
                'title': '${body.title ? body.title : ''}', 
                'description': '${body.description}', 
                'project_name': '${body.projectName ? body.projectName : ''}', 
                'question_id': '${questionId}', 
                'discord_channel_id': '${body.discordChannelId}', 
                'start_date': '${body.startDate ? body.startDate : ''}', 
                'end_date': '${body.endDate ? body.endDate : ''}', 
                'is_open': true,
                'voted': '0',
                'discord_message_id': '${discordMessageId ? discordMessageId : ''}',
                'is_posted_to_discord': ${body.sendToDiscordImmediately},
                'is_multi_select': ${body.isMultiSelect === undefined ? false : body.isMultiSelect},
                'multi_select_label': '${body.multiSelectLabel ? body.multiSelectLabel : ''}' ,`;

        if(body.contractAddress && body.tokenIds) {
            sql += `'contract_address': '${body.contractAddress ? body.contractAddress : ''}', 'token_ids': '${body.tokenIds ? body.tokenIds : ''}',`;
        }

        sql += `'artwork_ids': '${body.artworkIds ? body.artworkIds : ''}',
                'choices': ?,
                'created_date': '${new Date().toISOString()}'}`;
                    
        console.log("sql", sql);
        let statement = {Statement: sql, Parameters: [{S: JSON.stringify(body.choices)}]};
                        
        console.log(statement);
        let dbResult = await db.executeStatement(statement).promise();
        console.log("create vote question dbResult", dbResult);
        
        const response = {
            Success: true,
            Data: {
                voteId: questionId
            }
        };
        
        return response;
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-vote-discord-question-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-vote-discord-question-post - ' + random10DigitNumber,
            Message: `Error in ch-vote-discord-question-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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