import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import OpenAI from 'openai';
import axios from 'axios';
import jwt from 'jsonwebtoken';
import https from 'https';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

let tableName;
let configs;
let openai;

async function downloadJsonData(url) {
  try {
    const agent = new https.Agent({
      rejectUnauthorized: false, // Disables SSL verification (use only if necessary)
    });

    const response = await axios.get(url, { httpsAgent: agent });
    const jsonData = response.data; // The downloaded JSON data
    console.log('Downloaded JSON:', jsonData);
    return jsonData;
  } catch (error) {
    console.error('Error downloading JSON data:', error.message);
    throw error;
  }
}

export const handler = async (event) => {
  console.log("event", event);

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


    let token = headers['authorization'];
    console.log("token", token);

    let memberId = null;
    let member;

    if (!body.appPubKey && token) {
        try {
            const decoded = jwt.verify(token.split(' ')[1], configs.find(x=>x.key == 'JWT_SECRET').value);
            console.log("decoded", decoded);

            memberId = decoded.MemberId;

            if (Date.now() >= decoded.exp * 1000) {
                return {
                    Success: false,
                    Message: "Token expired"
                };
            }
        } catch (e) {
            console.log("error verify token", e);
            return {
                Success: false,
                Message: "Invalid token."
            };
        }

        let sql = `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = 'MEMBER_ID#${memberId}' AND type = 'MEMBER' AND begins_with("PK", 'MEMBER#')`;
        let memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if (memberResult.Items.length === 0) {
            console.log("member not found: " + memberId);
            return {
                Success: false,
                Message: "member not found: " + memberId
            };
        }
        member = memberResult.Items.map(item => unmarshall(item))[0];

        if (!member.role?.includes('ADMIN')) {
            return {
                Success: false,
                Message: "Unauthorized access"
            };
        }
    } else {
        return {
            Success: false,
            Message: "Missing login info"
        };
    }

    
    if(!body.jsonDataURL) {
      return {
        Success: false,
        Message: 'jsonDataURL is required'
      }
    }

    if(!body.instruction) {
      return {
        Success: false,
        Message: 'instruction is required'
      }
    }

    openai = new OpenAI({
                              apiKey: configs.find(x => x.key == 'OPENAI_APIKEY').value
                        });
    
    let json = await downloadJsonData(body.jsonDataURL);

    // let _instruction = body.instruction;
    let _instruction = body.instruction + ' ' + `Only source code is needed, no instruction or description. 
          Following is sample source data structure, but don't include these data in the code, instead, assume the source data is empty array, i.e. let data = [];  Output as full HTML page.`

    const response = await openai.chat.completions.create({
      model: "gpt-4", //"gpt-4",
      messages: [
        { role: "system", content: _instruction },
        { role: "user", content: `${JSON.stringify(json.slice(0, 20))}` },
      ],
    });

    let _html = response.choices[0].message.content.trim();
    console.log("_html", _html);
    
    _html = _html.includes('```') ? (_html.match(/```html([\s\S]*?)```/).length > 1 ? _html.match(/```html([\s\S]*?)```/)[1].trim() : _html.match(/```html([\s\S]*?)```/)[0].trim()) : _html;

    if (_html.startsWith('"') && _html.endsWith('"')) {
      return _html.slice(1, -1);  // Remove the first and last character
    }

    return {
      Success: true,
      Data: {
        html: _html,
        sourceData: json
      }
    };


  } catch (e) {
    const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;
    console.error('error in ch-analytic-chat-data-visualize ' + random10DigitNumber, e);

    const message = {
        Subject: 'Error - ch-analytic-chat-data-visualize - ' + random10DigitNumber,
        Message: `Error in ch-analytic-chat-data-visualize ${e.message}\n\nStack trace:\n${e.stack}`,
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