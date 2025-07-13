const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const { DynamoDBClient, ExecuteStatementCommand } = require("@aws-sdk/client-dynamodb");
const client = new DynamoDBClient({ region: process.env.AWS_REGION });
var jwt = require('jsonwebtoken');

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

function ToVoteAnswer(a) {
    return {
        DiscordUserId: a.SK,
        Voted: a.voted,
        QuestionStatement: a.question_statement,
        QuestionId: a.question_id,
        ChoiceIndex: a.is_multi_select ? a.choice_indexes : a.choice_index,
        ChoiceEmoji: a.is_multi_select ? a.choice_emojies : a.choice_emoji,
        ChoiceLabel: a.is_multi_select ? a.choice_labels : a.choice_label,
        ChoiceStatement: a.choice_statement,
        IsMultiSelect: a.is_multi_select,
        CreatedDate: a.created_date,
        Title: a.title,
        ProjectName: a.project_name
    };
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


        sql = `select * from "${tableName}"."ByTypeCreatedDate" WHERE type = 'VOTE_DISCORD_ANSWER'`;

        if(body.questionId != undefined) {
            sql += ` and PK = 'VOTE_DISCORD_QUESTION#${body.questionId}'`;
        }

        if(body.discordUserId != undefined) {
            sql += ` and discord_user_id = '${body.discordUserId}'`;
        }

        sql += ` order by created_date DESC`;

        console.log(sql);

        if(!body.pageSize) {
            body.pageSize = 10;
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

        // let decryptedMembers
        // if(body.isSimple)
        //     decryptedMembers = await Promise.all(allMembers.map(async(a) => await ToSimpleMemberViewModel(a)));    
        // else
        //     decryptedMembers = await Promise.all(allMembers.map(async(a) => await ToMemberViewModel(a)));

        if(allVotes.length > 0) {
            let questionPKs = allVotes.map(x => "'VOTE_DISCORD_QUESTION#" + x.question_id + "'").filter(onlyUnique);
            let questionsResult = await db.executeStatement({
                                                            Statement: `SELECT * FROM "${tableName}"."InvertedIndex" 
                                                                        WHERE PK IN (${questionPKs.join(", ")}) and SK = 'VOTE_DISCORD_QUESTION'`,
                                                        }).promise();

            if(questionsResult.Items.length > 0) {
                let questions = questionsResult.Items.map(unmarshall);
                console.log("questions", questions);
                // replace question in allVotes
                allVotes = allVotes.map(x => {
                                if(questions.filter(y => y.question_id == x.question_id).length > 0) {

                                    let question = questions.filter(c => c.question_id == x.question_id)[0];
                                    let isMultiSelect = question.is_multi_select;
                                    let choices = JSON.parse(question.choices);

                                    return {
                                        ...x, 
                                        question_statement: question.description, 
                                        choice_statement: isMultiSelect ? choices.filter((c, index) => x.choice_indexes.split(',').includes('' + index)) : choices[parseInt(x.choice_index)],
                                        is_multi_select: isMultiSelect,
                                        project_name: question.project_name,
                                        title: question.title
                                    }
                                }
                                else {
                                    return {...x, question_statement: "", choice_statement: "'"}
                                }
                            });
            }
            else {
                console.log("no question found. questionPKs : ", questionPKs);
            }
        }

        return {
                    Success: true,
                    Data: {
                        votes: allVotes.map(x => ToVoteAnswer(x)), 
                        lastKey: votesResult.LastEvaluatedKey 
                    }
                };
        
    } catch (e) {
        console.error('error in ch-vote-discord-answer-listing-get', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};