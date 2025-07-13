import { DynamoDBClient, ExecuteStatementCommand, ExecuteTransactionCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const snsClient = new SNSClient({ region: process.env.AWS_REGION });

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

function ToArtwork(obj) {
    return {
        Name: obj.name,
        Description: obj.description,
        ArtworkId: obj.artwork_id,
        MemberId: obj.user_id,
        ArtworkType: obj.artwork_type,
        Category: obj.category,
        SubCategory: obj.sub_category,
        Components: obj.components ? JSON.parse(obj.components) : undefined,
        NameEN: obj.name_en,
        ValueEN: obj.value_en,
        NameJP: obj.name_jp,
        ValueJP: obj.value_jp,
        Metadata: obj.metadata ? JSON.parse(obj.metadata) : undefined,
        Status: obj.status,
        TwoDURL: obj.two_d_url,
        TwoDMIME: obj.two_d_mime,
        TwoDURL_2: obj.two_d_url_2,
        TwoDMIME_2: obj.two_d_mime_2,
        TwoDURL_3: obj.two_d_url_3,
        TwoDMIME_3: obj.two_d_mime_3,
        ThreeDURL: obj.three_d_url,
        ThreeDMIME: obj.three_d_mime,
        CreatedDate: obj.created_date,
        LikedCount: obj.liked_count,
        Ranking: obj.ranking,
        VideoURL: obj.video_url,
        VideoMIME: obj.video_mime,
        Location: obj.location,
        Position: obj.position,
        Amount: obj.amount,
        Currency: obj.currency,
        DurationInMinutes: obj.duration_in_minutes,
        Expiry: obj.expiry,
        ReactionSummary: obj.reaction_summary ? JSON.parse(obj.reaction_summary) : undefined,
        Prompt: obj.prompt
    }
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
        let configResult = await dbClient.send(new ExecuteStatementCommand({Statement: `SELECT * FROM "${tableName}" WHERE PK = 'CONFIG'`}));
        configs = configResult.Items.map(unmarshall);
        console.log("configs", configs);

        if(!body.artworkId) {
            return {
                Success: false,
                Message: "artworkId is required"
            }
        }        

        let sql = `select * from "${tableName}" where PK = '${'ARTWORK#' + body.artworkId}' and type = 'ARTWORK'`;
        let artworkResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(artworkResult.Items.length == 0) {
            console.log("artwork not found: " + body.artworkId);
            const response = {
                Success: false,
                Message: "artwork not found: " + body.artworkId
            };
            return response;
        }
        let artwork = artworkResult.Items.map(unmarshall)[0];        
        
        console.log('get artwork like stamp');
        

        let likeStampsCount = [];

        sql = `select * from "${tableName}" where PK = 'ARTWORKFAVOURITE#${body.artworkId}' and type = 'ARTWORKFAVOURITE' `;
        let artworkFavResult = await dbClient.send(new ExecuteStatementCommand({Statement: sql}));
        if(artworkFavResult.Items.length > 0) {
            let artworkFavs = artworkFavResult.Items.map(unmarshall);
            let likeStamps = artworkFavs.map(x => x.like_stamp).filter(onlyUnique);   
            for (let i = 0; i < likeStamps.length; i++) {
                const stamp = likeStamps[i];
                if(stamp === undefined || stamp === '')
                    continue;

                let currentStampCount = artworkFavs.filter(x => x.like_stamp === stamp).length;
                likeStampsCount.push({Stamp: stamp, Count: currentStampCount})
            }
        }
        
        let artworkModel = ToArtwork(artwork);
        artworkModel.LikeStampCount = likeStampsCount;

        return {
            Success: true,
            Data: {
                artwork: artworkModel
            }
        };
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-artwork-get ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-artwork-get - ' + random10DigitNumber,
            Message: `Error in ch-artwork-get: ${e.message}\n\nStack trace:\n${e.stack}`,
            TopicArn: configs.find(x=>x.key=='SNS_TOPIC_ERROR').value
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