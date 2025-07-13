import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import jwt from 'jsonwebtoken';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });

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

        if(body.pageSize === undefined){
            return {
                Success: false,
                Message: 'pageSize is required',
            };
        }

        sql = `select * from "${tableName}"."ByTypeCreatedDate" WHERE type = 'SURVEY_ANSWER' `;
        
        if(body.surveyId) {
            sql += ` and survey_id = '${body.surveyId}'`;
        }

        if(body.memberId) {
            sql += ` and user_id = '${body.memberId}'`;
        }

        if(body.questionIndex) {
            sql += ` and question_index = '${body.questionIndex}'`;
        }

        if(body.lastKey && body.lastKey != '')
            sql += ` AND created_date < '${body.lastKey}'`;

        sql += ` order by created_date DESC`;
        
        console.log("sql", sql);
        
        if(!body.pageSize)
            body.pageSize = 10;
        
        var nextToken = null;
        var allAnswers = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var answerResult = null;
        while (attempt < maxAttempts) {
            answerResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = answerResult.NextToken;
            const answers = answerResult.Items.map(unmarshall);
            allAnswers.push(...answers);

            attempt++;

            if (!nextToken || allAnswers.length >= body.pageSize) break;
        }
        
        // let surveyMemberAnswersResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        // if(surveyMemberAnswersResult.Items.length == 0) {
        if(allAnswers.length == 0) {
            return {
                Success: false,
                Message: "survey answer not found"
            };
        }

        //let surveyMemberAnswers = surveyMemberAnswersResult.Items.map(unmarshall);
        //console.log("surveyMemberAnswers", surveyMemberAnswers);
        console.log("allAnswers", allAnswers);

        let surveysResult = await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY'`}));
        let surveys = surveysResult.Items.map(unmarshall);
        console.log("surveys", surveys);
        let surveyQuestionsResult = await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY_QUESTION'`}));
        let surveyQuestions = surveyQuestionsResult.Items.map(unmarshall);
        console.log("surveyQuestions", surveyQuestions);
        let surveyQuestionAnswersResult = await dbClient.send(new ExecuteStatementCommand({Statement: `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY_QUESTION_ANSWER'`}));
        let surveyQuestionAnswers = surveyQuestionAnswersResult.Items.map(unmarshall);
        console.log("surveyQuestionAnswers", surveyQuestionAnswers);

        let reportsurveyMemberAnswers = [];

        for(let i = 0; i < allAnswers.length; i++) {

            let question = surveyQuestions.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index);

            let answerEN = '';
            let answerJP = '';

            console.log("i", i);
            console.log("question", question);
            console.log("allAnswers[i]", allAnswers[i]);
            

            if(question.answer_type == 'SELECTION' || question.answer_type == 'RATING') {
                if(allAnswers[i].offered_answer_index.includes(',')) {
                    let answerIndexes = allAnswers[i].offered_answer_index.split(',');
                    for (let j = 0; j < answerIndexes.length; j++) {
                        const _answer = answerIndexes[j];
                        console.log("j", j);
                        console.log("_answer", _answer);
                        
                        answerEN += surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == _answer).answer_en + ',';
                        answerJP += surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == _answer).answer_jp + ',';
                    }
                    if(answerEN && answerEN.slice(-1) === ',')
                        answerEN = answerEN.substring(0, answerEN.length - 1);
                    if(answerJP && answerJP.slice(-1) === ',')
                        answerJP = answerJP.substring(0, answerJP.length - 1);
                }
                else {
                    answerEN = surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == allAnswers[i].offered_answer_index)?.answer_en;
                    answerJP = surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == allAnswers[i].offered_answer_index)?.answer_jp;
                }
            }

            let reportsurveyMemberAnswer = {
                MemberId: allAnswers[i].user_id,
                WalletAddress: allAnswers[i].wallet_address,
                DiscordId: allAnswers[i].discord_user_id,
                SurveyId: allAnswers[i].survey_id,
                SurveyTitle: surveys.find(x=> x.PK == `SURVEY#${allAnswers[i].survey_id}`).title,
                SurveyDescription: surveys.find(x=> x.PK == `SURVEY#${allAnswers[i].survey_id}`).description,
                QuestionIndex: allAnswers[i].question_index,
                QuestionEN: surveyQuestions.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index).question_en,
                QuestionJP: surveyQuestions.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index).question_jp,
                AnswerIndex: allAnswers[i].offered_answer_index ? allAnswers[i].offered_answer_index : '',
                AnswerText: allAnswers[i].offered_answer_text ? allAnswers[i].offered_answer_text : '',
                AnswerEN: answerEN,
                AnswerJP: answerJP,
                // AnswerEN: allAnswers[i].offered_answer_index ? surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == allAnswers[i].offered_answer_index).answer_en : '',
                // AnswerJP: allAnswers[i].offered_answer_index ? surveyQuestionAnswers.find(x => x.PK == `SURVEY#${allAnswers[i].survey_id}` && x.question_index == allAnswers[i].question_index && x.answer_index == allAnswers[i].offered_answer_index).answer_jp : '',
                CreatedDate: allAnswers[i].created_date
            }

            reportsurveyMemberAnswers.push(reportsurveyMemberAnswer);
        }
        
        return {
                    Success: true,
                    Data: { 
                        answers: reportsurveyMemberAnswers, 
                        lastKey: answerResult.LastEvaluatedKey 
                    },
                    
                };
        
    } catch (e) {
        console.error('error in ch-survey-member-answer-listing-post', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};