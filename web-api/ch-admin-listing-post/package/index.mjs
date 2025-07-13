import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import jwt from 'jsonwebtoken';
// import axios from 'axios';
// import * as jose from 'jose';
// import md5 from 'md5';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

async function ToMemberViewModel(obj){
    return {
        MemberId: obj.user_id,
        DisplayName: obj.display_name,
        DisplayNameKanji: obj.display_name_kanji,
        FirstName: obj.first_name,
        LastName: obj.last_name,
        PhotoURL: obj.photo_url,
        Biodata: obj.biodata,
        Email: obj.email,
        Phone: obj.phone,
        Provider: obj.provider,
        IsConsent: obj.is_consent,
        ProfilePictureURL: obj.profile_picture_url,
        Role: obj.role && obj.role !== '' ? obj.role : null,
        LastLogin: obj.last_login,
        ResetPwdToken: obj.reset_pwd_token,
        ResetPwdExpiry: obj.reset_pwd_expires,
        Status: obj.status,
        WalletAddress: obj.wallet_address,
        XPTotal: obj.xp_total,
        XPLevel: obj.xp_level,
        Settings: obj.settings,
        ConsentDate: obj.consent_date
    };
}

export const handler = async (event) => {
    
    console.log("admin listing get event", event);
    
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

        let configResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'`}));
        configs = configResult.Items.map(unmarshall);
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
        let memberResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(memberResult.Items.length == 0) {
            console.log("member not found: " + memberId);
            const response = {
                Success: false,
                Message: "member not found: " + memberId
            };
            return response;
        }
        let member = memberResult.Items.map(unmarshall)[0];

        if(member.role != "ADMIN") {
            console.log("Unauthorized access role for memberId " + memberId);
            const response = {
                Success: false,
                Message: "Unauthorized access role for memberId " + memberId
            };
            return response;
        }

        sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'MEMBER' and role = 'ADMIN'`;

        if(body.memberId)
            sql += ` AND player_id = '${body.memberId}'`;

        if(body.lastKey && body.lastKey != '')
            sql += ` AND created_date < '${body.lastKey}'`;

        sql += ` ORDER BY created_date DESC`;

        console.log("sql", sql);
        
        if(!body.pageSize)
            body.pageSize = 10;
        
        var nextToken = null;
        var allMembers = [];
        var maxAttempts = 40;    // max page size
        var attempt = 0;
        var membersResult = null;
        while (attempt < maxAttempts) {
            memberResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = memberResult.NextToken;
            const members = memberResult.Items.map(unmarshall);
            allMembers.push(...members);

            attempt++;

            if (!nextToken || allMembers.length >= body.pageSize) break;
        }
        
        let decryptedMembers = await Promise.all(allMembers.map(async(a) => await ToMemberViewModel(a)));
        
        const response = {
            Success: true,
            Data: { 
                    members: decryptedMembers, 
                    nextToken: nextToken
                }
        };
        
        return response;
        
    } catch (e) {
        console.error('error in ch-admin-listing-post', e);
        
        const response = {
            Success: false,
            Message: "Unexpected error had occured. Please contact your administrator."
        };
        
        return response;
    }
    
};