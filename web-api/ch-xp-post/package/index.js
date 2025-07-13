const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const ULID = require('ulid');
var jwt = require('jsonwebtoken');

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


        if(!body.discordId) {
            return {
                Success: false,
                Message: "discordId is required"
            };
        }

        if(!body.xpA) {
            return {
                Success: false,
                Message: "description is required"
            };
        }
        
        if(body.xpB == undefined) {
            return {
                Success: false,
                Message: "startDate is required"
            };
        }

        // if(body.rankA == undefined) {
        //     return {
        //         Success: false,
        //         Message: "rankA is required"
        //     };
        // }

        // if(body.rankB == undefined) {
        //     return {
        //         Success: false,
        //         Message: "rankA is required"
        //     };
        // }

        let xpId = ULID.ulid();

        let txStatements = [];

        sql = `INSERT INTO "${tableName}" 
                VALUE { 
                'PK': 'XP#${xpId}', 
                'SK': '${body.discordId}', 
                'type': 'XP', 
                'xp_a': '${body.xpA}', 
                'xp_b': '${body.xpB}', `;
                
        if(body.rankA) {
            sql += ` 'rank_a': '${body.rankA}', `;
        }

        if(body.rankB) {
            sql += ` 'rank_b': '${body.rankB}', `;
        }

        sql += `'created_date': '${new Date().toISOString()}'}`;

        txStatements.push({ "Statement": sql });

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("insert xp", dbTxResult);

        return {
                    Success: true
                };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-xp-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-xp-post - ' + random10DigitNumber,
            Message: `Error in ch-xp-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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