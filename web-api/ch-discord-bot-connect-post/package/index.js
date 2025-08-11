const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const axios = require("axios");
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();
const jwt = require("jsonwebtoken");

let tableName;
let configs;

const sleep = (ms) => {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
}

function toRoleId(roleName) {

    switch(roleName) {
        case 'AURORA':
            throw new Error('Please set role id for AURORA')
        default:
            console.log('Unexpected Role Name ' + roleName);
            return '';
    }
}

function toTestRoleId(roleName) {

    switch(roleName) {
        case 'AURORA':
            return '1402274605175144469'
        default:
            console.log('Unexpected Role Name ' + roleName);
            return '';
    }
}

const mintCarNFT = async (params, origin, token) => {
    console.log("mintCarNFT", params);
    let response = await axios.post(configs.find(x=>x.key == 'API_URL').value + '/nft/queue',
                        JSON.stringify(params),
                        {
                            headers: {
                                'Content-Type': 'application/json',
                                'origin': origin,
                                'authorization': 'Bearer ' + token
                            }
                        }
                    );
    console.log('mintCarNFT jsonResult', response.data);
    return response.data;
}

async function fetchAllRecords(sql) {
    let results = [];
    let nextToken;

    do {
        const params = {
            Statement: sql,
            NextToken: nextToken, // Include NextToken if available
        };

        const result = await db.executeStatement(params).promise();

        // Accumulate items from this page
        if (result.Items) {
            results = results.concat(result.Items);
        }

        // Update nextToken for the next iteration
        nextToken = result.NextToken;
    } while (nextToken); // Continue until there's no nextToken

    return results;
}

exports.handler = async (event) => {
    console.log("discord bot connect", event);
    
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

        let configResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'`}).promise();
        configs = configResult.Items.map(AWS.DynamoDB.Converter.unmarshall);
        console.log("configs", configs);
        
        if(body.interactionId === undefined){
            const response = {
                Success: false,
                Message: 'interactionId is required',
            };
            return response;
        }
        

        let member;

        if(body.appPubKey) {
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
    
            let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}).promise();
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Code: 1,
                    Message: 'member not found',
                };
            }
    
            member = memberResult.Items.map(AWS.DynamoDB.Converter.unmarshall)[0];
        }
        else {
            return {
                Success: false,
                //Message: "Missing login info"
                Code: 5,
                Message: "エラーが発生します。リロードしてもう一度お試しください"
            }
        }
        
        let discordInteractionResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'DISCORD#${body.interactionId}' and type = 'DISCORD' and interaction_type = 'JOIN'`}).promise();
        console.log("discordInteractionResult", JSON.stringify(discordInteractionResult));
        if(discordInteractionResult.Items.length === 0) {
            console.log('Discord Interaction not found. Discord インタラクションが見つかりません');
            return {
                Success: false,
                Code: 4,
                Message: 'Discord インタラクションが見つかりません',
            };
        }

        let discordInteraction = discordInteractionResult.Items.map(AWS.DynamoDB.Converter.unmarshall)[0];
        // let discordData = JSON.parse(discordInteraction.data);

        if(discordInteraction.status === 'DONE' && discordInteraction.wallet_address && discordInteraction.wallet_address != member.wallet_address) {
            console.log('The Discord Id is already linked to another account. Discord ID はすでに別のアカウントにリンクされています。');
            return {
                Success: false,
                Code: 2,
                Message: 'Discord ID はすでに別のアカウントにリンクされています。'
            }
        }

        let sql = `SELECT * FROM "${tableName}"."ByTypeCreatedDate" WHERE wallet_address = '${member.wallet_address}' and type = 'DISCORD' and status = 'DONE'`;
        let walletsConnectedResult = await fetchAllRecords(sql);
        console.log("walletsConnectedResult", JSON.stringify(walletsConnectedResult));
        if(walletsConnectedResult.length > 0) {
            let walletsConnected = walletsConnectedResult.map(AWS.DynamoDB.Converter.unmarshall)[0];

            if(walletsConnected.SK !== discordInteraction.SK){
                console.log('Your web3Auth wallet is already connected to another Discord account. あなたの web3Auth ウォレットはすでに別の Discord アカウントに接続されています');
                return {
                    Success: false,
                    Code: 3,
                    Message: 'あなたの web3Auth ウォレットはすでに別の Discord アカウントに接続されています',
                };
            }
        }
        
        if(member.discord_roles) {

            console.log("member.discord_roles", member.discord_roles);
            
            // grant role in discord

            const GUILD_ID = configs.find(x => x.key === 'DISCORD_GUILD_ID').value;
            const BOT_TOKEN = configs.find(x => x.key === 'DISCORD_BOT_TOKEN').value;
                    
            let discordRoleIds = tableName == process.env.TABLE_NAME_TEST 
                                        ? member.discord_roles.split(',').map(x => toTestRoleId(x)).filter(id => id !== '') 
                                        : member.discord_roles.split(',').map(x => toRoleId(x)).filter(id => id !== '');

            console.log("discordRoleIds", discordRoleIds);

            for(let i = 0; i < discordRoleIds.length; i++) {

                let roleId = discordRoleIds[i];

                try {
                
                    let url = `https://discord.com/api/v10/guilds/${GUILD_ID}/members/${discordInteraction.SK}/roles/${roleId}`
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
                    console.log("grant discord role result", grantRoleResult);

                    // mint car NFT
                    const token = jwt.sign({ MemberId: '01GJ5XT15FHWPFRN5QJSPXKW0X' }, configs.find(x => x.key == 'JWT_SECRET').value, { expiresIn: '1440m' });
                    const carArtwordIds = ["01K00Y29HBE51ABZFJDGFB4X3R","01K00WYNTDQM1KVZ8NWPK786C1","01K00RPAR68XC1HGBKXCM3FAXY","01K009MQ32GNCMH1C8MQK8HG1P"];
                    let mintCarQueueEn = await mintCarNFT({
                                                            "nftType": "CAR",  // MEMBER_A, MEMBER_B, , CAR, CHARACTER
                                                            "queueType": "MINT_QUEUE",   // MINT_QUEUE or UPGRADE_QUEUE
                                                            "artworkId": carArtwordIds[Math.floor(Math.random() * carArtwordIds.length)],
                                                            "memberId": member.user_id
                                                            },
                                                            headers['origin'],
                                                            token
                                                        );
                    console.log('mintCarQueueEn', mintCarQueueEn);

                } catch (err) {
                    console.log(err);
                    const _message = {
                        Subject: 'CH Error - ch-discord-bot-connect-post',
                        Message: "unable to grant discord role id " + roleId + " for discord user id " + discordInteraction.SK,
                        TopicArn: configs.find(x => x.key === 'SNS_TOPIC_ERROR').value
                    };
                    await sns.publish(_message).promise();
                }

                await sleep(1000);
            }
        }

        // get discord global name
        let discordUser;
        try {
                
            const GUILD_ID = configs.find(x => x.key === 'DISCORD_GUILD_ID').value;
            const BOT_TOKEN = configs.find(x => x.key === 'DISCORD_BOT_TOKEN').value;
            
            let url = `https://discord.com/api/v10/guilds/${GUILD_ID}/members/${discordInteraction.SK}`
            console.log('get discord user url', url);
            let _headers = {
                                "Authorization": `Bot ${BOT_TOKEN}`,
                                "Content-Type": "application/json"
                            };
            let userResult = await axios.get(url,
                                                {
                                                    headers: _headers,
                                                });
            console.log("get discord user result", userResult);

            discordUser = userResult.data;
            
        } catch (err) {
            console.log(err);
            const _message = {
                Subject: 'CH Error - ch-discord-bot-connect-post',
                Message: "failed to get discord user " + discordInteraction.SK,
                TopicArn: configs.find(x => x.key === 'SNS_TOPIC_ERROR').value
            };
            await sns.publish(_message).promise();
        }

        let txStatements = [];

        txStatements.push({"Statement": 
            `update "${tableName}" set discord_user_id = '${discordInteraction.SK}', `
                + 
                (discordUser && discordUser.user.global_name ? ` display_name = '${discordUser.user.global_name}', ` : '')
                +
            ` modified_date = '${new Date().toISOString()}' where PK = '${member.PK}' and SK = '${member.SK}'`
        });
        
        txStatements.push({"Statement": `update "${tableName}" set status = 'DONE', user_id = '${member.user_id}', wallet_address = '${member.wallet_address}' , modified_date = '${new Date().toISOString()}' WHERE PK = '${discordInteraction.PK}' and SK = '${discordInteraction.SK}'`});

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        
        const dbTxResult = await db.executeTransaction(statements).promise();
        console.log("Update dbResult", dbTxResult);
        
        return  {
            Success: true
        };
        
    } catch (e) {
        console.error('error in ch-discord-bot-connect-post', e);

        const message = {
            Subject: 'CH Error - ch-discord-bot-connect-post',
            Message: `Error in ch-discord-bot-connect-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key === 'SNS_TOPIC_ERROR').value
        };

        if(tableName == process.env.TABLE_NAME)
            await sns.publish(message).promise();
        
        let msg = 'エラーが発生しました。管理者に連絡してください。';
        if(e.message.includes('429')){
            msg = '短期間に大量のユーザがボットにアクセスすると、Discordの仕様で、エラーが発生することがあります。エラーが解消しない場合は、しばらくしてから再度お試しください。';
        }

        const response = {
            Success: false,
            Code: 99,
            Message: msg
        };
        
        return response;
    }
    
};