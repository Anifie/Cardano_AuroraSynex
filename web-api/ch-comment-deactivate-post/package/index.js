const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: '2012-08-10'});
const ULID = require('ulid');
const axios = require("axios");
var jwt = require('jsonwebtoken');
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();

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
        
        let memberId = null;
        let member;

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
        member = memberResult.Items.map(unmarshall)[0];

        if(member.role !== 'ADMIN') {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }

        let artwork;
        let asset;

        if(!body.commentId) {
            return {
                Success: false,
                Message: 'commentId is required'
            }
        }

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${'COMMENT#' + body.commentId}' and type = 'COMMENT'`;
        let commentResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(commentResult.Items.length == 0) {
            console.log("comment not found: " + body.commentId);
            return {
                Success: false,
                Message: "comment not found: " + body.commentId
            };
        }
        let comment = commentResult.Items.map(unmarshall)[0];
        console.log("comment", comment);

        sql = `update "${tableName}" set status = 'INACTIVE' , modified_date = '${new Date().toISOString()}' where PK = '${comment.PK}' and SK = '${comment.SK}'`;
        let updateResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        console.log("updateResult", updateResult);
        
        return {
            Success: true
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-comment-deactivate-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-comment-deactivate-post - ' + random10DigitNumber,
            Message: `Error in ch-comment-deactivate-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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