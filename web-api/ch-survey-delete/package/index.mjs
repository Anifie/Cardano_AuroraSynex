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
        let surveys = surveyResult.Items.map(unmarshall);
        console.log("surveys", surveys);

        let _survey = surveys.find(x=> x.type == 'SURVEY');
        console.log("_survey", _survey);
        let _surveyQuestions = surveys.filter(x=> x.type == 'SURVEY_QUESTION');
        console.log("_surveyQuestions", _surveyQuestions);
        let _surveyQuestionAnswers = surveys.filter(x=> x.type == 'SURVEY_QUESTION_ANSWER');
        console.log("_surveyQuestionAnswers", _surveyQuestionAnswers);

        txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${_survey.PK}' and SK = '${_survey.SK}'` });

        for(let i = 0; i < _surveyQuestions.length; i++) {
            let question = _surveyQuestions[i];
            txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${question.PK}' and SK = '${question.SK}'` });

            sql = `select * from "${tableName}"."InvertedIndex" where SK = 'SURVEY#${_survey.SurveyId}#QUESTION#${question.question_index}' and type = 'SURVEY_ANSWER'`;
            let memberAnswerResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            let memberAnswers = memberAnswerResult.Items.map(unmarshall);

            for(let j = 0; j < memberAnswers.length; j++) {
                let memberAnswer = memberAnswers[j];
                txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${memberAnswer.PK}' and SK = '${memberAnswer.SK}'` });
            }
        }

        for(let i = 0; i < _surveyQuestionAnswers.length; i++) {
            let questionAnswer = _surveyQuestionAnswers[i];
            txStatements.push({ "Statement": `DELETE FROM "${tableName}" WHERE PK = '${questionAnswer.PK}' and SK = '${questionAnswer.SK}'` });
        }

        const statements = { "TransactStatements": txStatements };  
        console.log("statements", JSON.stringify(statements));
        const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
        console.log("delete survey", dbTxResult);

        return {
                    Success: true
                };
        
    } catch (e) {
        console.error('error in ch-survey-delete', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};