const AWS = require('aws-sdk');
const db = new AWS.DynamoDB({region: process.env.AWS_REGION, apiVersion: process.env.DYNAMODB_API_VERSION});
var jwt = require('jsonwebtoken');

function ToXP(a) {
    return {
        XPId: a.PK,
        DiscordId: a.SK,
        XP_A: a.xp_a,
        XP_B: a.xp_b,
        XP_total: a.xp_total != undefined ? a.xp_total : (parseInt(a.xp_a != undefined ? a.xp_a : 0) + parseInt(a.xp_b != undefined ? a.xp_b : 0)),
        Common: a.common,
        RankA: a.rank_a,
        RankB: a.rank_b,
        CreatedDate: a.created_date
    };
}

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

        // let sql = `select * from "${process.env.TABLE_NAME}"."InvertedIndex" where SK = 'MEMBER_ID#${memberId}' and type = 'MEMBER' and begins_with("PK", 'MEMBER#')`;
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


        

        sql = `select * from "${tableName}"."ByTypeCreatedDate" WHERE type = 'XP' order by created_date DESC`;
        
        // if(body.voteProposalId) {
        //     sql += ` PK = 'VOTE_PROPOSAL#${body.voteProposalId}' `;
        // }
        // else {
        //     sql += ` BEGINS_WITH("PK", 'VOTE_PROPOSAL#')`;
        // }
        
        let xpResult = await dbClient.send(new ExecuteStatementCommand({ Statement: sql }));
        if(xpResult.Items.length == 0) {
            console.log("xp not found");
            return {
                Success: false,
                Message: "XP not found"
            };
        }

        let xpData = xpResult.Items.map(unmarshall);
        
        let _xpData = xpData.map(x => ToXP(x));
        console.log("_xpData", _xpData);

        return {
                    Success: true,
                    Data: _xpData
                };
        
    } catch (e) {
        console.error('error in ch-xp-listing-post', e);
        
        const response = {
            Success: false,
            Message: JSON.stringify(e),
        };
        
        return response;
    }
    
};