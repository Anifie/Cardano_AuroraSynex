const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const { DynamoDBClient, ExecuteStatementCommand } = require("@aws-sdk/client-dynamodb");
const client = new DynamoDBClient({ region: process.env.AWS_REGION });
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
            }
        }

        sql = `select * from "${tableName}" WHERE PK = 'VOTE_DISCORD_QUESTION#${body.questionId}' and type = 'VOTE_DISCORD_QUESTION'`;
        let questionResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(questionResult.Items.length == 0) {
            console.log("question not found: " + memberId);
            return {
                Success: false,
                Message: "question not found: " + memberId
            };
        }
        let question = questionResult.Items.map(unmarshall)[0];

        sql = `select * from "${tableName}" WHERE PK = 'VOTE_DISCORD_QUESTION#${body.questionId}' and type = 'VOTE_DISCORD_ANSWER'`;

        console.log(sql);

        if(!body.pageSize) {
            body.pageSize = 1000;
        }

        var nextToken = null;
        var allVotes = [];
        var maxAttemps = 40;    // max page size
        var attempt = 0;
        var votesResult = null;
        while(attempt < maxAttemps) {
            votesResult = await client.send(
                                                    new ExecuteStatementCommand({
                                                        Statement: sql,
                                                        NextToken: nextToken,
                                                        Limit: +body.pageSize
                                                    })
                                                );

            console.log("votesResult", JSON.stringify(votesResult));
            console.log("votesResult.NextToken", votesResult.NextToken);
            console.log("votesResult.LastEvaluatedKey", votesResult.LastEvaluatedKey);
            
            nextToken = votesResult.NextToken;
        
            var votes = votesResult.Items.map(unmarshall);
            allVotes.push(...votes);
            console.log("allVotes", JSON.stringify(allVotes));
            console.log("allVotes length", allVotes.length);
            console.log("attempt", attempt);

            attempt++;
            
            if(votesResult.NextToken == null 
                || votesResult.NextToken == undefined
                || allVotes.length >= body.pageSize)
                break;
        }

        let choices = JSON.parse(question.choices);

        if(question.is_multi_select) {
            for (let i = 0; i < choices.length; i++) {
                
                choices[i].count = 0;

                for (let j = 0; j < allVotes.length; j++) {
                    const vote = allVotes[j];
                    if(vote.choice_indexes.split(',').includes('' + i)) {
                        choices[i].count ++;
                    }
                }
            }
        }
        else {
            for (let i = 0; i < choices.length; i++) {
                let count = allVotes.reduce((accumulator, currentChoice) => {
                    return (currentChoice.choice_index == i) ? accumulator + 1 : accumulator
                }, 0)
                choices[i].count = count;
            }
        }
        
        return {
                    Success: true,
                    Data: {
                        choices: choices
                    }
                };
        
    } catch (e) {
        console.error('error in ch-vote-discord-answer-statistic-get', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};