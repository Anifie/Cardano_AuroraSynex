import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import OpenAI from 'openai';

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });

let tableName;
let configs;
let openai;

async function analyzeMessage(instruction, message) {
  try {
    const response = await openai.chat.completions.create({
      model: "gpt-3.5-turbo", //"gpt-4",
      messages: [
        { role: "system", content: instruction }, //"You are an analyst scoring user messages for enthusiasm (1-5) and knowledge depth (1-5)."
        { role: "user", content: `Rate the following message: ${message}` },
      ],
    });

    const result = response.choices[0].message.content.trim();
    const _result = JSON.parse(result);
    return _result;

    // const [enthusiasm, knowledge] = result.split(",").map(Number);
    // return { enthusiasm, knowledge };

  } catch (error) {
    console.error(`Error analyzing message: ${message}`, error);

    return '';
    // return { enthusiasm: null, knowledge: null }; // Graceful fallback
  }
}

export const handler = async (event) => {
  console.log("event", event);

  try {
    
    if(typeof event == 'string')
      event = JSON.parse(event);
      
      if(event.IsTest) {
        tableName = process.env.TABLE_NAME_TEST;
      }
      else 
        tableName = process.env.TABLE_NAME;
      
      console.log("tableName", tableName);
      
      let configResult = await dbClient.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'` }));
      configs = configResult.Items.map(item => unmarshall(item));
      console.log("configs", configs);
      
      openai = new OpenAI({
                                apiKey: configs.find(x => x.key == 'OPENAI_APIKEY').value
                          });

      let analysis = await analyzeMessage(event.Instruction, event.Messages);
      console.log('analysis', analysis);

      // const userIdRegex = /<@(\d{17,20})>/g; // Matches only user mentions , with user id 17 to 20 digits
      const userIdRegex = /<@(\d+)>/g;  // Matches <@UserID> with any length

      let userIds = new Set(); // Use a Set to avoid duplicates
      const matches = [...event.Messages.matchAll(userIdRegex)];
      matches.forEach(match => userIds.add(match[1]));
      const matchesArray = Array.from(userIds); // Convert Set to Array

      analysis = {...analysis, related_user_ids: matchesArray}

      return analysis;
      

      // const batchResults = await Promise.all(
      //   userMessages[i].messages.map(async msg => {
      //     const analysis = await analyzeMessage(msg.message);
      //     return { 
      //       message_id: msg.message_id, 
      //       message: msg.message, 
      //       ...analysis 
      //     };
      //   })
      // );

      // console.log("batchResults", batchResults);
      
      // userMessages[i].result = batchResults;


  } catch (error) {
    console.error("Error in Lambda function:", error);
    throw error;
  }
};