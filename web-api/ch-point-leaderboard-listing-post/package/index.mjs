import { DynamoDBClient, ExecuteStatementCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import axios from 'axios';

// Initialize clients
const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function toLeaderBoard(obj) {
    return {
        MemberId: obj.user_id,
        DiscordUserId: obj.discord_user_id,
        DiscordUserName: obj.discord_user_name,
        WalletAddress: obj.wallet_address,
        Level: obj.level,
        TotalPoints: obj.total_points,
        MessagesCount: obj.messages_count,
        ReactionsCount: obj.reactions_count,
        VotesCount: obj.votes_count,
        AttachmentsCount: obj.attachments_count,
        Roles: obj.roles && obj.roles.length > 0 ? JSON.parse(obj.roles) : undefined,
        PointsRequiredToNextLevel: obj.points_required_to_next_level,
        Rank: obj.rank,
        Rank2: obj.rank_2,
        NFTCount: obj.nft_count,
        RepliesCount: obj.replies_count,
        CreatedDate: obj.created_date,
    }
}

export const handler = async (event) => {
    // console.log("nft dequeue mint event", event);
    
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

        let sql = `select * from "${tableName}"."ByTypeCreatedDate" where type = 'LEADERBOARD' order by created_date`;
        if(body.discordUserId) {
            sql = `select * from "${tableName}"."InvertedIndex" where type = 'LEADERBOARD' and SK = '${body.discordUserId}'`;
        }

        console.log("sql", sql);


        if(!body.pageSize){
            body.pageSize = 10;
        }

        // Execute the SQL query
        let nextToken = body.nextToken;
        let allLeaderboards = [];
        const maxAttempts = 40;
        let attempt = 0;

        while (attempt < maxAttempts) {
            const leaderboardResult = await dbClient.send(
                new ExecuteStatementCommand({
                    Statement: sql,
                    NextToken: nextToken,
                    Limit: +body.pageSize
                })
            );

            nextToken = leaderboardResult.NextToken;
            const leaderboards = leaderboardResult.Items.map(unmarshall);
            allLeaderboards.push(...leaderboards);

            attempt++;

            if (!nextToken || allLeaderboards.length >= body.pageSize) break;
        }

        return {
            Success: true,
            Data: {
                leaderboards: allLeaderboards.map(a => toLeaderBoard(a)),
                nextToken: nextToken
            }
        };

        // let leaderboardsResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        // let leaderBoards = leaderboardsResult.Items.map(unmarshall);

        // return {
        //     Success: true,
        //     Data: leaderBoards.map(x => toLeaderBoard(x))
        // }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-point-leaderboard-listing-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-point-leaderboard-listing-post - ' + random10DigitNumber,
            Message: `Error in ch-point-leaderboard-listing-post: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x => x.key == 'SNS_TOPIC_ERROR').value
        };
        
        if(tableName == process.env.TABLE_NAME)
             await snsClient.send(new PublishCommand(message));
        
        const response = {
            Success: false,
            Message: e.message
            //Message: 'エラーが発生しました。管理者に連絡してください。Code: ' + random10DigitNumber
        };
        
        return response;
    }
    
};