const AWS = require('aws-sdk');
const ULID = require('ulid');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
const axios = require("axios");
var jwt = require('jsonwebtoken');
const jose = require("jose");
const md5 = require("md5");
const sns = new AWS.SNS();

export const handler = async (event) => {
    console.log("nft enqueue mint event", event);
    
    let tableName;

    try {
        
        var headers = event.headers;
        var body = {};

        if(event.body)
            body = JSON.parse(event.body);

        console.log("origin", headers['origin']);
        console.log("referer", headers['referer']);
        let tableName = process.env.TABLE_NAME;
        if(headers['origin'] == undefined) {
            headers['origin'] = headers['referer'];
        }
        if((!headers['origin'].includes("anifie.community.admin") && !headers['origin'].includes("honda-synergy-lab.jp") && !headers['origin'].includes("anifie.com") && !headers['origin'].includes("global.honda")) || (headers['origin'].includes("anifie.communitytest.admin.s3-website-ap-northeast-1.amazonaws.com"))) {
            tableName = process.env.TABLE_NAME_TEST;
        }
        console.log("tableName", tableName);

        var token = headers['authorization'];
        console.log("token", token);
        
        let memberId = null;
        let member;

        if(body.appPubKey == undefined && token) {
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


            // replace member with member who we want to sent the NFT to
            if(body.memberId == undefined) {
                return {
                    Success: false,
                    Message: "memberId is required"
                };
            }            
            sql = `select * from "${tableName}" where PK = 'MEMBER#${body.memberId}' and type = 'MEMBER'`;
            memberResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
            if(memberResult.Items.length == 0) {
                console.log("member not found: " + body.memberId);
                const response = {
                    Success: false,
                    Message: "member not found: " + body.memberId
                };
                return response;
            }
            member = memberResult.Items.map(unmarshall)[0];
        
        }
        else if(body.appPubKey) {

            var token = headers['authorization'];
            console.log("token", token);

            if(!token)  {
                console.log('missing authorization token in headers');
                const response = {
                        Success: false,
                        Code: 1,
                        Message: "Unauthorize user"
                    };
                return response;
            }

            let userId;
            // let aggregateVerifier;
        
            //verify token
            try{
                const idToken = token.split(' ')[1] || "";
                const jwks = jose.createRemoteJWKSet(new URL("https://api.openlogin.com/jwks"));
                const jwtDecoded = await jose.jwtVerify(idToken, jwks, {
                                                                            algorithms: ["ES256"],
                                                                        });
                console.log("jwtDecoded", JSON.stringify(jwtDecoded));
        
                if ((jwtDecoded.payload).wallets[0].public_key == body.appPubKey) {
                    // Verified
                    console.log("Validation Success");
                } else {
                    // Verification failed
                    console.log("Validation Failed");
                    return {
                        Success: false,
                        Code: 1,
                        Message: "Validation failed"
                    };
                }
                
                userId = await md5(jwtDecoded.payload.verifierId + "#" + jwtDecoded.payload.aggregateVerifier)
                console.log("userId", userId);
                
                // aggregateVerifier = jwtDecoded.payload.aggregateVerifier;
                
            }catch(e){
                console.log("error verify token", e);
                const response = {
                    Success: false,
                    Code: 1,
                    Message: "Invalid token."
                };
                return response;
            }

            let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + userId }],}).promise();
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        else if (body.uuid && body.token) {
            
            const _response = await axios.post(process.env.PARTICLE_API_URL,
                {
                    jsonrpc: "2.0",
                    id: 0,
                    method: "getUserInfo",
                    params: [body.uuid, body.token],
                },
                {
                    auth: {
                        username: process.env.PARTICLE_PROJECT_ID,
                        password: process.env.PARTICLE_SERVER_KEY,
                    },
                }
            );

            console.log(_response.data);

            if(_response.data.error) {
                console.log("Error verify user info", _response.data.error);
                throw new Error(JSON.stringify(_response.data.error))
            }

            let memberResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}" WHERE PK = ? and type = 'MEMBER'`, Parameters: [{ S: 'MEMBER#' + body.uuid }],}).promise();
            console.log("memberResult", JSON.stringify(memberResult));
            if(memberResult.Items.length === 0) {
                return {
                    Success: false,
                    Message: 'member not found',
                };
            }

            member = memberResult.Items.map(unmarshall)[0];
        }
        else {
            return {
                Success: false,
                Code: 1,
                Message: "Missing login info."
            };
        }

        let award;
        let isWinner = false;

        let sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'WHITELIST_MEMBER_SILVER_LITTLEBLUE'`;
        console.log("sql", sql);
        let whiteListASilverResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'WHITELIST_MEMBER_GOLD_LITTLEBLUE'`;
        console.log("sql", sql);
        let whiteListAGoldResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'WHITELIST_MEMBER_SILVER_METAFORGE'`;
        console.log("sql", sql);
        let whiteListBSilverResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'WHITELIST_MEMBER_GOLD_METAFORGE'`;
        console.log("sql", sql);
        let whiteListBGoldResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_LITTLEBLUE'`;
        console.log("sql", sql);
        let awardLittleBlueResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_FINANCIE'`;
        console.log("sql", sql);
        let awardFinancieResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_MIZUNOSHINYA'`;
        console.log("sql", sql);
        let awardMizunoResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_AURORA_1'`;
        console.log("sql", sql);
        let awardHonda1Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_AURORA_2'`;
        console.log("sql", sql);
        let awardHonda2Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        sql = `select * from "${tableName}"."InvertedIndex" where SK = '${member.PK}' and type = 'WHITELIST' and whitelist_type = 'LITTLEBLUE_WINNER_AWARD_AURORA_3'`;
        console.log("sql", sql);
        let awardHonda3Result = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));

        let memberContractAddr = (tableName == process.env.TABLE_NAME_TEST ? process.env.CONTRACT_ADDRESS_HONDA721M_TEST : process.env.CONTRACT_ADDRESS_HONDA721M);
        let membershipNFTsResult = await db.executeStatement({Statement: `SELECT * FROM "${tableName}"."InvertedIndex" WHERE SK = '${member.SK}' and type = 'ASSET' and contract_address = '${memberContractAddr}' and status = 'NOTFORSALE'`}).promise();
        console.log("membershipNFTsResult", JSON.stringify(membershipNFTsResult));
        if(membershipNFTsResult.Items.length === 0) {
            return {
                Success: false,
                Message: 'Membership NFT not found.'
            };
        }

        let AStatus, BStatus;
        let membershipNFTs = membershipNFTsResult.Items.map(unmarshall);
        for (let i = 0; i < membershipNFTs.length; i++) {
            const memberNFT = membershipNFTs[i];
            console.log("memberNFT", memberNFT);
            if(memberNFT.store_id == 'AURORA_MEMBERSHIP_A') {
                if(memberNFT.is_gold === true) {
                    AStatus = 'GOLD'
                }
                else if(memberNFT.is_silver === true) {
                    AStatus = 'SILVER'
                }
                else {
                    AStatus = 'BRONZE'
                }
            }
            else if(memberNFT.store_id == 'AURORA_MEMBERSHIP_B') {
                if(memberNFT.is_gold === true) {
                    BStatus = 'GOLD'
                }
                else if(memberNFT.is_silver === true) {
                    BStatus = 'SILVER'
                }
                else {
                    BStatus = 'BRONZE'
                }
            }
        }

        if(awardLittleBlueResult.Items.length > 0) {
            award = 'LITTLEBLUE';
            isWinner = true;
        }
        else if(awardFinancieResult.Items.length > 0) {
            award = 'FINANCIE';
            isWinner = true;
        }
        else if(awardMizunoResult.Items.length > 0) {
            award = 'MIZUNOSHINYA';
            isWinner = true;
        }
        else if(awardHonda1Result.Items.length > 0 || awardHonda2Result.Items.length > 0 || awardHonda3Result.Items.length > 0) {
            award = 'HONDA';
            isWinner = true;
        }
        
        return {
            Success: true,
            Data: {
                isASilverWhiteListed: whiteListASilverResult.Items.length > 0,
                isAGoldWhiteListed: whiteListAGoldResult.Items.length > 0,
                isBSilverWhiteListed: whiteListBSilverResult.Items.length > 0,
                isBGoldWhiteListed: whiteListBGoldResult.Items.length > 0,
                isWinner: isWinner,
                award: award,
                AStatus: AStatus,
                BStatus: BStatus
            }
        }
        
    } catch (e) {
        const random10DigitNumber = Math.floor(Math.random() * 9000000000) + 1000000000;

        console.error('error in ch-nft-whitelist-validate-post ' + random10DigitNumber, e);
        
        const message = {
            Subject: 'Honda Cardano Error - ch-nft-whitelist-validate-post - ' + random10DigitNumber,
            Message: `Error in ch-nft-whitelist-validate-post: ${e.message}\n\nStack trace:\n${e.stack}`,
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