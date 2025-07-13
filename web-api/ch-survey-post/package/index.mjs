import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import jwt from 'jsonwebtoken';
import ULID from 'ulid';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });

function chunkArray(array, chunkSize) {
    const result = [];
    for (let i = 0; i < array.length; i += chunkSize) {
        result.push(array.slice(i, i + chunkSize));
    }
    return result;
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

        if(!body.title) {
            return {
                Success: false,
                Message: "title is required"
            };
        }

        if(!body.description) {
            return {
                Success: false,
                Message: "description is required"
            };
        }
        
        if(body.isOpen == undefined) {
            return {
                Success: false,
                Message: "isOpen is required"
            };
        }

        if(body.questions == undefined) {
            return {
                Success: false,
                Message: "questions is required"
            };
        }

        let surveyId = ULID.ulid();

        let txStatements = [];

        sql = `INSERT INTO "${tableName}" 
                VALUE { 
                'PK': 'SURVEY#${surveyId}', 
                'SK': 'SURVEY', 
                'type': 'SURVEY', 
                'title': '${body.title}', 
                'survey_id': '${surveyId}', 
                'description': '${body.description}', 
                'start_date': '${body.startDate ? body.startDate : ''}', 
                'end_date': '${body.endDate ? body.endDate : ''}', 
                'is_open': '${body.isOpen}',
                'created_date': '${new Date().toISOString()}'}`;
        txStatements.push({ "Statement": sql });

        for(let i = 0; i < body.questions.length; i++) {
            let question = body.questions[i];
            sql = `INSERT INTO "${tableName}" 
                    VALUE { 
                    'PK': 'SURVEY#${surveyId}', 
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
                sql = `INSERT INTO "${tableName}" 
                        VALUE { 
                        'PK': 'SURVEY#${surveyId}', 
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
        
        const chunkedTxStatements = chunkArray(txStatements, 100);

        for (let i = 0; i < chunkedTxStatements.length; i++) {
            const chunk = chunkedTxStatements[i];
            const statements = { "TransactStatements": chunk };
            console.log(`Executing chunk ${i + 1}/${chunkedTxStatements.length}`);
            
            try {
                const dbTxResult = await dbClient.send(new ExecuteTransactionCommand(statements));
                console.log("Insert success for chunk", i + 1, dbTxResult);
            } catch (err) {
                console.error("Error inserting chunk", i + 1, err);
                // Optional: handle retries or abort logic here
            }
        }

        return {
                    Success: true
                };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-survey-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-survey-post - ' + random10DigitNumber,
            Message: `Error in ch-survey-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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