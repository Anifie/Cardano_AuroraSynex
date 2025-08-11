import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
import axios from "axios";

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
}

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

    // // delete for test environment only
    // tableName = process.env.TABLE_NAME_TEST;

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
    } else {
        return {
            Success: false,
            Message: "Missing login info"
        };
    }

    let sql;

    if(body.discordUserId !== undefined){
        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${body.discordUserId}' and type = 'DISCORD'`;
        const discordResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(discordResult.Items.length > 0) {
            let discord = discordResult.Items.map(item => unmarshall(item))[0];
            if(discord.user_id) {
                body.memberId = discord.user_id;
            }
        }
    }

    if(body.walletAddress !== undefined){
        sql = `SELECT * FROM "${tableName}"."InvertedIndex" where SK = 'MEMBERWALLET#${body.walletAddress}' and type = 'MEMBER'`;
    }
    else if(body.memberId !== undefined){
        sql = `SELECT * FROM "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
    }
    else {
        throw new Error("Missing member info");
    }

    let txStatements = [];

    let memberResult = await fetchAllRecords(sql);
    member = memberResult.map(item => unmarshall(item))[0];

    // ASSET, MEMBER, QUEUE, ARTWORK with SK
    // SURVEY_ANSWER with PK
    // DISCORD , WHITELIST, ROLE_MEMBER with user_id

    sql = `delete from "${tableName}" where PK = '${member.PK}' and SK = '${member.SK}'`;
    txStatements.push({ "Statement": sql});

    sql = `select * from "${tableName}" where type <> 'MEMBER' and SK = '${member.SK}'`
    let result = await fetchAllRecords(sql);
    let data = result.map(item => unmarshall(item));
    for (let i = 0; i < data.length; i++) {
        const _data = data[i];
        sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
        txStatements.push({ "Statement": sql});    
    }

    sql = `select * from "${tableName}" where type <> 'MEMBER' and PK = '${member.PK}'`
    result = await fetchAllRecords(sql);
    data = result.map(item => unmarshall(item));
    for (let i = 0; i < data.length; i++) {
        const _data = data[i];
        sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
        txStatements.push({ "Statement": sql});    
    }

    sql = `select * from "${tableName}" where type = 'STICKER' and wallet_address = '${member.wallet_address}'`
    result = await fetchAllRecords(sql);
    data = result.map(item => unmarshall(item));
    for (let i = 0; i < data.length; i++) {
        const _data = data[i];
        sql = `update "${tableName}" set is_used = false, wallet_address = '' where PK = '${_data.PK}' and SK = '${_data.SK}'`
        txStatements.push({ "Statement": sql});    
    }

    sql = `select * from "${tableName}" where type = 'DISCORD' and user_id = '${member.user_id}'`
    result = await fetchAllRecords(sql);
    data = result.map(item => unmarshall(item));
    for (let i = 0; i < data.length; i++) {
        const _data = data[i];
        sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
        txStatements.push({ "Statement": sql});    
    }
    
    // sql = `select * from "${tableName}" where type = 'WHITELIST' and user_id = '${member.user_id}'`
    // result = await fetchAllRecords(sql);
    // data = result.map(item => unmarshall(item));
    // for (let i = 0; i < data.length; i++) {
    //     const _data = data[i];
    //     sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
    //     txStatements.push({ "Statement": sql});    
    // }

    sql = `select * from "${tableName}" where type = 'ROLE_MEMBER' and user_id = '${member.user_id}'`
    result = await fetchAllRecords(sql);
    if(result && result.length > 0) {
        data = result.map(item => unmarshall(item));
        for (let i = 0; i < data.length; i++) {
            const _data = data[i];
            sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
            txStatements.push({ "Statement": sql});    
        }
    }

    sql = `select * from "${tableName}" where type = 'MEMBER_CHATCHANNEL' and user_id = '${member.user_id}'`
    result = await fetchAllRecords(sql);
    if(result && result.length > 0) {
        data = result.map(item => unmarshall(item));
        for (let i = 0; i < data.length; i++) {
            const _data = data[i];
            sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
            txStatements.push({ "Statement": sql});    
        }
    }

    sql = `select * from "${tableName}"."InvertedIndex" where type = 'AVATAR' and SK = 'MEMBER#${member.user_id}'`
    result = await fetchAllRecords(sql);
    if(result && result.length > 0) {
        data = result.map(item => unmarshall(item));
        for (let i = 0; i < data.length; i++) {
            const _data = data[i];
            sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
            txStatements.push({ "Statement": sql});    
        }
    }

    // if(member.discord_user_id) {
    //     sql = `select * from "${tableName}" where type = 'VOTE_DISCORD_ANSWER' and SK = '${member.discord_user_id}'`
    //     result = await fetchAllRecords(sql);
    //     if(result && result.length > 0) {
    //         data = result.map(item => unmarshall(item));
    //         for (let i = 0; i < data.length; i++) {
    //             const _data = data[i];
    //             sql = `delete from "${tableName}" where PK = '${_data.PK}' and SK = '${_data.SK}'`
    //             txStatements.push({ "Statement": sql});    
    //         }
    //     }
    // }
    
    const chunkedArray = chunkArray(txStatements, 100);

    for (let i = 0; i < chunkedArray.length; i++) {
        const chuck = chunkedArray[i];
        
        const statements = { "TransactStatements": chuck };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("Transaction result", dbTxResult);
        
    }
    
    

    //kick member from discord
    if(member.discord_user_id_real) {
        try {
            const GUILD_ID = configs.find(x=>x.key == 'DISCORD_GUILD_ID').value;
            const BOT_TOKEN = configs.find(x=>x.key == 'DISCORD_BOT_TOKEN').value;
            
            let url = `https://discord.com/api/v8/guilds/${GUILD_ID}/members/${member.discord_user_id_real}`
            console.log('kick member url', url);
            let _headers = {
                                "Authorization": `Bot ${BOT_TOKEN}`,
                                "Content-Type": "application/json"
                            };
            let kickResult = await axios.delete(url,
                                                {
                                                    headers: _headers,
                                                });
            console.log("kick discord user result", kickResult);
            
        } catch (err) {
            console.log(err);
            const _message = {
                Subject: 'TD Error - td-admin-member-delete',
                Message: "unable to kick member from discord. discord user id " + member.discord_user_id_real + ' for nftType ' + body.nftType,
                TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
            };
            // await sns.publish(_message).promise();
            await snsClient.send(new PublishCommand(_message));
        }
    }

    return {
        Success: true
    }
    
  } catch (e) {
    const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
    console.error('error in ch-admin-member-delete  ' + random10DigitNumber, e);

    const message = {
        Subject: 'Error - ch-admin-member-delete - ' + random10DigitNumber,
        Message: `Error in ch-admin-member-delete  ${e.message}\n\nStack trace:\n${e.stack}`,
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