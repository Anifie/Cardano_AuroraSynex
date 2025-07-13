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

        if(!body.surveyId) {
            return {
                Success: false,
                Message: "surveyId is required"
            };
        }

        let txStatements = [];

        sql = `select * from "${tableName}" WHERE PK = 'SURVEY#${body.surveyId}'`;
        let surveyResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        let survey = surveyResult.Items.map(unmarshall).find(x=> x.type == 'SURVEY');
        console.log("survey", survey);

        sql = `update "${tableName}" set modified_date = '${new Date().toISOString()}'`;

        if(body.title) {
            sql += ` , title = '${body.title}'`;
        }

        if(body.isOpen != undefined) {
            sql += ` , is_open = '${body.isOpen}'`;
        }

        if(body.description) {
            sql += ` , description = '${body.description}'`;
        }

        sql += ` where PK = '${survey.PK}' and SK = '${survey.SK}'`;

        txStatements.push({ "Statement": sql });

        // delete all questions and answers, and re-insert later
        if(body.questions) {
            let _surveyQuestions = surveyResult.Items.map(unmarshall).filter(x=> x.type == 'SURVEY_QUESTION');
            console.log("_surveyQuestions", _surveyQuestions);
            let _surveyQuestionAnswers = surveyResult.Items.map(unmarshall).filter(x=> x.type == 'SURVEY_QUESTION_ANSWER');
            console.log("_surveyQuestionAnswers", _surveyQuestionAnswers);

            for(let i = 0; i < _surveyQuestions.length; i++) {
                let question = _surveyQuestions[i];
                txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${question.PK}' and SK = '${question.SK}'` });
            }
    
            for(let i = 0; i < _surveyQuestionAnswers.length; i++) {
                let questionAnswer = _surveyQuestionAnswers[i];
                txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${questionAnswer.PK}' and SK = '${questionAnswer.SK}'` });
            }
        }

        let statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        let dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));

        txStatements = [];

        if(body.questions) {
            body.questions = JSON.parse(body.questions);
            
            for(let i = 0; i < body.questions.length; i++) {
                let question = body.questions[i];
                
                console.log("i", i,question);

                sql = `INSERT INTO "${tableName}" 
                        VALUE { 
                        'PK': 'SURVEY#${body.surveyId}', 
                        'SK': 'QUESTION#${question.index}', 
                        'type': 'SURVEY_QUESTION', 
                        'question_index': '${question.index}',
                        'question_en': '${question.text_en}', 
                        'question_jp': '${question.text_jp}',
                        'mandatory': ${question.mandatory},
                        'remark': '${question.remark}',
                        'answer_type': '${question.answer_type}',
                        'created_date': '${new Date().toISOString()}'}`;
                txStatements.push({ "Statement": sql });
    
                for(let j = 0; j < question.answers.length; j++) {
                    let answer = question.answers[j];
                    console.log("j", j,answer);
                    sql = `INSERT INTO "${tableName}" 
                            VALUE { 
                            'PK': 'SURVEY#${body.surveyId}', 
                            'SK': 'QUESTION#${question.index}#ANSWER${answer.index}', 
                            'type': 'SURVEY_QUESTION_ANSWER', 
                            'question_index': '${question.index}',
                            'answer_index': '${answer.index}',
                            'answer_en': '${answer.text_en}', 
                            'answer_jp': '${answer.text_jp}',
                            'answer_remark': '${answer.remark ? answer.remark : ''}',
                            'created_date': '${new Date().toISOString()}'}`;
                    txStatements.push({ "Statement": sql });
                }
            }
        }

        statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("update survey", dbTxResult);

        return {
                    Success: true
                };
        
    } catch (e) {
        console.error('error in ch-survey-put', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};