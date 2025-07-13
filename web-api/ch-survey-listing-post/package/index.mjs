import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
// import jwt from 'jsonwebtoken';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });

function ToSurvey(a) {
    return {
        SurveyId: a.survey_id,
        Title: a.title,
        Description: a.description,
        IsOpen: a.is_open,
        StartDate: a.start_date,
        EndDate: a.end_date,
        CreatedDate: a.created_date,
        Completed: a.completed
    };
}

function ToQuestion(a) {
    return {
        index: a.question_index,
        text_en: a.question_en,
        text_jp: a.question_jp,
        mandatory: a.mandatory,
        remark: a.remark,
        answer_type: a.answer_type
        // CreatedDate: a.created_date
    };
}

function ToAnswer(a) {
    return {
        index: a.answer_index,
        text_en: a.answer_en,
        text_jp: a.answer_jp,
        remark: a.remark
        // CreatedDate: a.created_date
    };
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
        console.log("tableName", tableName);

        let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
        configs = configResult.Items.map(item => unmarshall(item));
        console.log("configs", configs);

        // var token = headers['authorization'];
        // console.log("token", token);
        
        // if(!token)  {
        //     console.log('missing authorization token in headers');
        //     const response = {
        //             Success: false,
        //             Message: "Unauthorize user"
        //         };
        //     return response;
        // }
        
        // let memberId = null;
        
        // //verify token
        // try{
        //     const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key=='JWT_SECRET').value);
        //     console.log("decoded", decoded);
            
        //     memberId = decoded.PlayerId;
            
        //     if (Date.now() >= decoded.exp * 1000) {
        //         const response = {
        //             Success: false,
        //             Message: "Token expired"
        //         };
        //         return response;
        //     }
        // }catch(e){
        //     console.log("error verify token", e);
        //     const response = {
        //         Success: false,
        //         Message: "Invalid token."
        //     };
        //     return response;
        // }

        // let sql = `select * from "${tableName}"."InvertedIndex" where SK = 'MEMBER_ID#${memberId}' and type = 'MEMBER' and begins_with("PK", 'MEMBER#')`;
        // let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        // if(memberResult.Items.length == 0) {
        //     console.log("member not found: " + memberId);
        //     const response = {
        //         Success: false,
        //         Message: "member not found: " + memberId
        //     };
        //     return response;
        // }
        // let member = memberResult.Items.map(unmarshall)[0];

        // if(member.role !== 'ADMIN') {
        //     return {
        //         Success: false,
        //         Message: "Unauthorized access"
        //     };
        // }


        

        let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY' and is_open = 'true'`;
        
        if(body.surveyId) {
            sql += ` and PK = 'SURVEY#${body.surveyId}'`;
        }
        
        console.log(sql);

        let surveyResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(surveyResult.Items.length == 0) {
            console.log("survey not found: " + body.surveyId);
            const response = {
                Success: false,
                Message: "survey not found: " + body.surveyId
            };
        }

        let surveys = surveyResult.Items.map(unmarshall);
        
        let surveyArr = []

        let _surveys = surveys.filter(x=> x.type == 'SURVEY');
        
        console.log("_surveys", _surveys);
        

        for(let i =0; i<_surveys.length; i++) {
            let survey = ToSurvey(_surveys[i]);
            survey.Questions = [];

            sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY_QUESTION' and PK = 'SURVEY#${survey.SurveyId}'`
            let surveyQuestionResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            let _surveyQuestions = surveyQuestionResult.Items.map(unmarshall);
            console.log("_surveyQuestions", _surveyQuestions);

            sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'SURVEY_QUESTION_ANSWER' and PK = 'SURVEY#${survey.SurveyId}'`
            let surveyQuestionAnswerResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            let _surveyQuestionAnswers = surveyQuestionAnswerResult.Items.map(unmarshall);            
            console.log("_surveyQuestionAnswers", _surveyQuestionAnswers);

            for(let i=0; i<_surveyQuestions.length; i++) {
                let question = ToQuestion(_surveyQuestions[i]);
                question.answers = [];
                let questionAnswers = _surveyQuestionAnswers.filter(x=> x.question_index == question.index)
                console.log("questionAnswers", questionAnswers);
                for(let j=0; j<questionAnswers.length;j++) {
                    let answer = ToAnswer(questionAnswers[j]);
                    console.log("answer", answer);
                    question.answers.push(answer);
                }
                survey.Questions.push(question)
            }
            
            surveyArr.push(survey);
        }        

        return {
                    Success: true,
                    Data: surveyArr
                };
        
    } catch (e) {
        
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-survey-listing-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-survey-listing-post - ' + random10DigitNumber,
            Message: `Error in ch-survey-listing-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };

        await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};