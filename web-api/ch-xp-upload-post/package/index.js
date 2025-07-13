const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const ULID = require('ulid');
var jwt = require('jsonwebtoken');
const sns = new AWS.SNS();

export const handler = async (event) => {
    console.log("event", event);
    
    let tableName;

    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);
            
        var token = headers['authorization'];
        console.log("token", token);

        console.log("origin", headers['origin']);
        tableName = process.env.TABLE_NAME;
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);
        
        if(!token)  {
            console.log('missing authorization token in headers');
            const response = {
                    Success: false,
                    Message: "Unauthorize user"
                };
            return response;
        }
        
        let memberId = null;
        
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

        if(member.role !== 'ADMIN') {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }

        if(!body.xpData) {
            return {
                Success: false,
                Message: "xpData is required"
            }
        }

        //let txStatements = [];

        for (let i = 0; i < body.xpData.length; i++) {
            const row = body.xpData[i];
            console.log("i : ", i);
            console.log(row);

            if(!row.DiscordId) {
                console.log("Missing DiscordId for row number " + (i+1));
                return {
                    Success: false,
                    Message: "Missing DiscordId for row number " + (i+1)
                }
            }
                
            sql = `select * from "${tableName}"."InvertedIndex" where SK = '${row.DiscordId}' and type = 'XP'`;
            let existingXPResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(existingXPResult.Items.length > 0) {
                console.log('Updating, Member XP data already exist for row number ' + (i+1), row);

                let existingXP = existingXPResult.Items.map(unmarshall)[0];

                sql = `UPDATE "${tableName}" SET modified_date = '${new Date().toISOString()}' `;
                
                if(row.XP_A && row.XP_A !== '') {
                    sql += `, xp_a = '${row.XP_A}' `
                }

                if(row.XP_B && row.XP_B !== '') {
                    sql += `, xp_b = '${row.XP_B}' `
                }

                if(row.Rank_A && row.Rank_A !== '') {
                    sql += `, rank_a = '${row.Rank_A}' `
                }

                if(row.Rank_B && row.Rank_B !== '') {
                    sql += `, rank_b = '${row.Rank_B}' `
                }

                if(row.XP_total && row.XP_total !== '') {
                    sql += `, xp_total = '${row.XP_total}' `
                }

                if(row.Common && row.Common !== '') {
                    sql += `, common = '${row.Common}' `
                }

                sql += ` WHERE PK = '${existingXP.PK}' AND SK = '${existingXP.SK}'`;
                
                console.log("sql", sql);
    
                let insertResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("updateResult", insertResult);
            }        
            else {
                let xpId = ULID.ulid();
            
                sql = `INSERT INTO "${tableName}" 
                    VALUE { 
                    'PK': 'XP#${xpId}', 
                    'SK': '${row.DiscordId}', 
                    'type': 'XP', 
                    'xp_a': '${row.XP_A}', 
                    'xp_b': '${row.XP_B}', `;

                if(row.Rank_A && row.Rank_A !== '') {
                    sql += ` 'rank_a': '${row.Rank_A}', `;
                }

                if(row.Rank_B && row.Rank_B !== '') {
                    sql += ` 'rank_b': '${row.Rank_B}', `;
                }

                if(row.XP_total && row.XP_total !== '') {
                    sql += ` 'xp_total': '${row.XP_total}', `;
                }

                if(row.Common && row.Common !== '') {
                    sql += ` 'common': '${row.Common}', `;
                }

                sql +=  `'created_date': '${new Date().toISOString()}'}`;
    
                //txStatements.push({ "Statement": sql });
                console.log("sql", sql);
                let insertResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
                console.log("insertResult", insertResult);
            }
        }

        // if(txStatements.length > 0) {
        //     const statements = { "TransactStatements": txStatements };  
        //     console.log("statements", JSON.stringify(statements));
        //     const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        //     console.log("insert whitelist", dbTxResult);
        // }

        return {
                    Success: true
                };
       
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-xp-upload-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-xp-upload-post  - ' + random10DigitNumber,
            Message: `Error in ch-xp-upload-post : ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };

        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: e.message
        };
        
        return response;
    }
    
};