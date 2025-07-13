import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

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

        if(body.password == undefined){
            const response = {
                Success: false,
                Message: 'password is required',
            };        
            return response;
        }

        if(body.username == undefined){
            const response = {
                Success: false,
                Message: 'username is required',
            };        
            return response;
        }

        let memberResult = await dbClient.send(new ExecuteStatementCommand({
                                                        Statement: `
                                                            SELECT * FROM "${tableName}"
                                                            WHERE PK = '${'MEMBER#LOCAL#' + body.username.toLowerCase()}'
                                                        `
                                                    }))

        console.log("memberResult", memberResult);

        if(memberResult.Items.length == 0) {
            console.log("user not found " + body.username.toLowerCase());
            const response = {
                Success: false,
                Message: "user not found " + body.username.toLowerCase(),
            };        
            return response;
        }

        const member = memberResult.Items.map(unmarshall)[0];
        const validPassword = await bcrypt.compare(body.password, member.password);
        if(!validPassword) {
            console.log("Wrong Password for " + body.username.toLowerCase());
            const response = {
                Success: false,
                Message: "Wrong Password",
            };        
            return response;
        }

        if(member.role !== 'ADMIN' && member.role !== 'OPERATOR') {
            console.log("Unauthorized access for member " + body.username);
            const response = {
                Success: false,
                Message: "Unauthorized access for member " + body.username,
            };        
            return response;
        }

        //update last_login
        let updateResult = await dbClient.send(new ExecuteStatementCommand({
                                                        Statement: `
                                                            UPDATE "${tableName}"
                                                            SET last_login = '${new Date().getTime()}'
                                                            WHERE PK = '${member.PK}' and SK = '${member.SK}'
                                                        `
                                                    }));

        const token = jwt.sign({ MemberId: member.member_id }, configs.find(x=>x.key=='JWT_SECRET').value, { expiresIn: '1440m' });  // expired in 1 day
        console.log("generated token", token);
        const decoded = jwt.verify(token, configs.find(x=>x.key=='JWT_SECRET').value);
        console.log("decoded", decoded);
        
        const response = {
            Success: true,
            Data: {
                token: token,
                profile: {
                            DisplayName: member.display_name,
                            MemberId: member.member_id,
                            Email: body.username,
                            Avatar: member.avatar_uri,
                            Role: member.role
                        }
            }
        };
        
        return response;

    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
        console.error('error in ch-admin-signin-post ' + random10DigitNumber, e);
    
        const message = {
            Subject: 'Honda Cardano Error - ch-admin-signin-post - ' + random10DigitNumber,
            Message: `Error in ch-admin-signin-post  ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
            await snsClient.send(new PublishCommand(message));
    
        return {
            Success: false,
            Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
    }
};