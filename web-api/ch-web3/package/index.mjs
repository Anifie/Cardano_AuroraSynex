import { DynamoDBClient, ExecuteStatementCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import {
  // resolveNativeScriptAddress,
  resolveNativeScriptHash,
  Transaction,
  // Data,
  // fromText,
  // toHex,
  ForgeScript,
  BlockfrostProvider,
  MeshTxBuilder,
  MeshWallet,
  serializePlutusScript,
  resolvePlutusScriptHash,
  resolvePlutusScriptAddress,
  resolvePaymentKeyHash,
  resolveScriptHash,
  deserializeAddress
  // UTxO
} from "@meshsdk/core";
import axios from "axios";
import md5 from "md5";

// import { NativeScript } from "@meshsdk/common"
// import mesh from "@meshsdk/core";
// import {MeshPlutusNFTContract} from "@meshsdk/contract"

// import { mnemonicToEntropy, entropyToMnemonic, mnemonicToSeedSync } from "bip39";
// import { Bip32PrivateKey, BaseAddress, NetworkInfo, StakeCredential } from "@emurgo/cardano-serialization-lib-browser";

const dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });

const blockchainProvider = new BlockfrostProvider(process.env.BLOCKFROST_PROJECT_ID);
const mnemonicArray = process.env.ADMIN_WALLET_MNEMONIC.split(" "); //mnemonic.split(" ");
// console.log("mnemonicArray", mnemonicArray);
const adminCardanoWallet = new MeshWallet({
    networkId: 1,   //0 is testnet, 1 is mainnet
    fetcher: blockchainProvider,
    submitter: blockchainProvider,
    key: {
    type: 'mnemonic',
    words: mnemonicArray,
    },
});
// console.log("adminCardanoWallet", adminCardanoWallet);

const blockchainProviderTest = new BlockfrostProvider(process.env.BLOCKFROST_PROJECT_ID_TEST);
const mnemonicArrayTest = process.env.ADMIN_WALLET_MNEMONIC_TEST.split(" "); //mnemonic.split(" ");
// console.log("mnemonicArray", mnemonicArray);
const adminCardanoWalletTest = new MeshWallet({
    networkId: 1,   //0 is testnet, 1 is mainnet
    fetcher: blockchainProviderTest,
    submitter: blockchainProviderTest,
    key: {
    type: 'mnemonic',
    words: mnemonicArrayTest,
    },
});
// console.log("adminCardanoWalletTest", adminCardanoWalletTest);


const KOIOS_MAINNET = 'https://api.koios.rest/api/v1';

async function getWalletBalance(address) {
  try {
    const response = await axios.post(`${KOIOS_MAINNET}/address_info`, {
      _addresses: [address]
    }, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    const data = response.data;

    if (!data.length) {
      console.log('No balance found for this address.');
      return;
    }

    const lovelace = data[0].balance;
    const ada = Number(lovelace) / 1_000_000;

    console.log(`Balance: ${ada} ADA`);
    return ada;
  } catch (error) {
    console.error('Error fetching wallet balance:', error);
  }
}

export const handler = async (event) => {

    console.log("event", event);

    if(event.action === undefined){
        throw new Error("Action is required");
    }

    switch (event.action) {          

        case "BURN_CAR":
            return await BurnCar(event.unit, event.isTest);

        case "BURN_CHARACTER":
            return await BurnCharacter(event.unit, event.isTest);

        case "BURN_MEMBERSHIP":
            return await BurnMembership(event.unit, event.isTest);

        case "UPDATE_MEMBERSHIP_METADATA":
            return await UpdateMembershipMetadata(event.unit, event.metadata, event.isTest);

        case "UPDATE_CAR_METADATA":
            return await UpdateCarMetadata(event.unit, event.metadata, event.isTest);

        case "UPDATE_CHARACTER_METADATA":
            return await UpdateCharacterMetadata(event.unit, event.metadata, event.isTest);

        case "MINT_MEMBER": 
            return await MintMember(event.toAddress, event.metadata, event.isTest);

        case "MINT_CAR": 
            return await MintCar(event.toAddress, event.metadata, event.isTest);

        case "MINT_CHARACTER": 
            return await MintCharacter(event.toAddress, event.metadata, event.isTest);

        case "MEMBERSHIP_OWNER": 
            return await CheckMembershipOwnerOf(event.unit, event.isTest);

        case "REVOKE_MEMBER": 
            return await RevokeMemberNFT(event.unit, event.isTest);

        case "REVOKE_CHARACTER": 
            return await RevokeCharacterNFT(event.unit, event.isTest);

        case "REVOKE_CAR": 
            return await RevokeCarNFT(event.unit, event.isTest);

        case "MINT_RACINGFAN": 
            return await MintRacingFan(event.toAddress, event.metadata, event.isTest);

        case "BURN_RACINGFAN":
            return await BurnRacingFan(event.unit, event.isTest);

        case "UPDATE_RACINGFAN_METADATA":
            return await UpdateRacingFanMetadata(event.unit, event.metadata, event.isTest);
        
        case "REVOKE_RACINGFAN":
            return await RevokeRacingFanNFT(event.unit, event.isTest);

        default:
            throw new Error("Not supported action " + event.action); 
    }
};

async function BurnCar(unit, isTest) {
    let burnResult = await BurnNFT(unit, isTest);
    console.log("burnResult", burnResult);

    return {
        burnResult: burnResult
    }
}

async function BurnCharacter(unit, isTest) {
    let burnResult = await BurnNFT(unit, isTest);
    console.log("burnResult", burnResult);
}

async function BurnMembership(unit, isTest) {
    let burnResult = await BurnNFT(unit, isTest);
    console.log("burnResult", burnResult);
}

// CIP-68
// async function UpdateMembershipMetadata(unit, metadata, isTest) {
//     console.log("UpdateMembershipMetadata", unit, metadata, isTest);

//     // burn & re-mint reference nft

//     let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;
//     console.log("admin wallet address", await wallet.getChangeAddress());
//     let adminAddr = await wallet.getChangeAddress();
//     console.log("adminAddr", adminAddr);

//     const policyId = unit.slice(0, 56);
//     console.log("policyId", policyId);
    
//     const assetNameHex = unit.slice(56);
//     console.log("assetNameHex", assetNameHex);
    
//     const assetName = Buffer.from(assetNameHex, 'hex').toString('utf8');
//     console.log("assetName", assetName);

//     const referenceAssetName = assetName.replace("222", "100");
//     const referenceAssetNameHex = Buffer.from(referenceAssetName, 'utf8').toString('hex');
//     console.log("referenceAssetNameHex", referenceAssetNameHex);

//     const referenceUnit = policyId + referenceAssetNameHex;
//     console.log("referenceUnit", referenceUnit);
    
//     const { pubKeyHash: keyHash } = deserializeAddress(await wallet.getChangeAddress());
//     console.log("keyHash", keyHash);
    
//     const nativeScript = {
//         type: "sig",
//         keyHash: keyHash
//     };

//     // ⚠️ Validate derived policyId matches expected policyId
//     const derivedPolicyId = resolveNativeScriptHash(nativeScript);
//     if (derivedPolicyId !== policyId) {
//         throw new Error(`Mismatch in policyId: expected ${policyId}, got ${derivedPolicyId}`);
//     }

//     const forgingScript = ForgeScript.fromNativeScript(nativeScript);

//     let tx = new Transaction({ initiator: wallet });

//     tx.burnAsset(
//             forgingScript,
//             {
//                 unit: referenceUnit,
//                 quantity: 1
//             }
//         );
        
//     let unsignedTx = await tx.build();
//     let signedTx = await wallet.signTx(unsignedTx);
//     let txHash = await wallet.submitTx(signedTx);
//     console.log("txHash", txHash);
//     let response = await waitForTx(txHash, isTest);
//     console.log("response", response);



//     // re-mint
//     tx = new Transaction({ initiator: wallet });

//     let referenceAsset = {   
//       assetName: '100' + metadata.name,
//       assetQuantity: '1',
//       metadata: metadata,
//       recipient: adminAddr
//     };
//     console.log("reference asset", referenceAsset);

//     tx.mintAsset(
//             forgingScript,
//             referenceAsset,
//         );

//     unsignedTx = await tx.build();
//     signedTx = await wallet.signTx(unsignedTx);
//     txHash = await wallet.submitTx(signedTx);
//     console.log("txHash", txHash);
//     response = await waitForTx(txHash, isTest);
//     console.log("response", response);


//     return {
//         response,
//         transactionHash: txHash,
//         policyId: derivedPolicyId,
//         assetName,
//         unit
//     };
// }

async function UpdateMembershipMetadata(unit, metadata, isTest) {
    console.log("UpdateMembershipMetadata", unit, metadata, isTest);

    let burnResult = await BurnNFT68Reference(unit, isTest);
    console.log("burnResult", burnResult);
    
    await waitForTxConfirmed(burnResult.transactionHash, 6*5, 10000);   // 5 minutes, every 10 seconds

    let mintResultReference = await MintNFT68Reference(metadata, isTest);
    console.log("mintResultReference", mintResultReference);

    return mintResultReference;
}

async function UpdateCarMetadata(unit, metadata, isTest) {
    console.log("UpdateCarMetadata", unit, metadata, isTest);

    let burnResult = await BurnNFT68Reference(unit, isTest);
    console.log("burnResult", burnResult);
    
    await waitForTxConfirmed(burnResult.transactionHash, 6*5, 10000);   // 5 minutes, every 10 seconds

    let mintResultReference = await MintNFT68Reference(metadata, isTest);
    console.log("mintResultReference", mintResultReference);
}

async function UpdateCharacterMetadata(unit, metadata, isTest) {
    console.log("UpdateCharacterMetadata", unit, metadata, isTest);

    let burnResult = await BurnNFT68Reference(unit, isTest);
    console.log("burnResult", burnResult);
    
    await waitForTxConfirmed(burnResult.transactionHash, 6*5, 10000);   // 5 minutes, every 10 seconds

    let mintResultReference = await MintNFT68Reference(metadata, isTest);
    console.log("mintResultReference", mintResultReference);
}

async function MintMember(toAddress, metadata, isTest) {
    console.log("MintMember", toAddress, metadata, isTest);
    let mintResult = await MintNFT68(toAddress, metadata, isTest);
    console.log("mintResult", mintResult);
    return mintResult;
}

async function MintCar(toAddress, metadata, isTest) {
    console.log("MintCar", toAddress, metadata, isTest);
    let mintResult = await MintNFT68(toAddress, metadata, isTest);
    console.log("mintResult", mintResult);
    return mintResult;
}

async function MintCharacter(toAddress, metadata, isTest) {
    console.log("MintCharacter", toAddress, metadata, isTest);
    let mintResult = await MintNFT68(toAddress, metadata, isTest);
    console.log("mintResult", mintResult);
    return mintResult;
}

async function CheckMembershipOwnerOf(unit, isTest) {
    try {
        // Get UTXOs holding the asset
        const utxos = await blockchainProvider.fetchAssetAddresses(unit);

        if (utxos.length === 0) {
        console.log('NFT not found or already burned');
        return null;
        }

        // Typically, the first UTXO is the current holder
        const currentOwner = utxos[0].address;
        console.log(`Owner Address: ${currentOwner}`);
        return currentOwner;

    } catch (error) {
        console.error('Failed to fetch NFT owner:', error);
        return null;
    }
}

async function RevokeMemberNFT(unit, isTest) {
    console.log("RevokeMemberNFT", unit, isTest);
    let revokeResult = await BurnNFT68Reference(unit, isTest);
    console.log("revokeResult", revokeResult);
    return revokeResult;
}

async function RevokeCharacterNFT(unit, isTest) {
    console.log("RevokeCharacterNFT", unit, isTest);
    let revokeResult = await BurnNFT68Reference(unit, isTest);
    console.log("revokeResult", revokeResult);
    return revokeResult;
}
async function RevokeCarNFT(unit, isTest) {
    console.log("RevokeCarNFT", unit, isTest);
    let revokeResult = await BurnNFT68Reference(unit, isTest);
    console.log("revokeResult", revokeResult);
    return revokeResult;
}
async function MintRacingFan(toAddress, metadata, isTest) {
    console.log("MintRacingFan", toAddress, metadata, isTest);
    let mintResult = await MintNFT68(toAddress, metadata, isTest);
    console.log("mintResult", mintResult);
    return mintResult;
}
async function BurnRacingFan(unit, isTest) {
    console.log("BurnRacingFan", unit, isTest);
    let burnResult = await BurnNFT(unit, isTest);
    console.log("burnResult", burnResult);
    return burnResult;
}

async function UpdateRacingFanMetadata(unit, metadata, isTest) {
    console.log("UpdateRacingFanMetadata", unit, metadata, isTest);

    let burnResult = await BurnNFT68Reference(unit, isTest);
    console.log("burnResult", burnResult);
    
    await waitForTxConfirmed(burnResult.transactionHash, 6*5, 10000);   // 5 minutes, every 10 seconds

    let mintResultReference = await MintNFT68Reference(metadata, isTest);
    console.log("mintResultReference", mintResultReference);
}
async function RevokeRacingFanNFT(unit, isTest) {
    console.log("RevokeRacingFanNFT", unit, isTest);
    let revokeResult = await BurnNFT68Reference(unit, isTest);
    console.log("revokeResult", revokeResult);
    return revokeResult;
}

async function fetchTxStatus(txHash, isTest) {
    try {
        //const response = await axios.get(`https://cardano-preview.blockfrost.io/api/v0/txs/${txHash}`, {
        const response = await axios.get(`https://cardano-mainnet.blockfrost.io/api/v0/txs/${txHash}`, {
            headers: { project_id: isTest ? process.env.BLOCKFROST_PROJECT_ID_TEST : process.env.BLOCKFROST_PROJECT_ID },
        });

        console.log("response", response);
        return { status: "confirmed", data: response.data };
    } catch (error) {
        if (error.response && error.response.status === 404) {
            return { status: "pending" };
        } else {
            console.error("Error fetching transaction:", error.message);
            return { status: "failed" };
        }
    }
}

async function waitForTx(txHash, isTest, maxRetries = 30, delay = 5000) {
    for (let i = 0; i < maxRetries; i++) {
        const result = await fetchTxStatus(txHash, isTest);
        console.log(`Transaction Status: ${result.status}`);

        if (result.status === "confirmed") {
            console.log("Transaction confirmed!");
            return result.data;
        }

        await new Promise((resolve) => setTimeout(resolve, delay));
    }

    throw new Error("Transaction confirmation timeout exceeded.");
}

async function MintNFT(toAddress, metadata, isTest) {
    let _metadata = metadata;
    let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;

    if(typeof _metadata !== 'object') {
        _metadata = JSON.parse(_metadata);
    }

    if(!_metadata.name || !_metadata.name.trim()) {
        throw new Error("Metadata name is required");
    }

    if(!_metadata.attributes) {
        _metadata.attributes = [];
    }
    _metadata.attributes.push({
                    "trait_type": "SIGNATURE",
                    "value": md5(JSON.stringify(_metadata) + process.env.NFT_SECRET)
                });

    let asset = {   
      assetName: _metadata.name,
      assetQuantity: '1',
      metadata: _metadata,
      label: '721',
      recipient: toAddress
    }
    console.log("asset", asset);
    
    const { pubKeyHash: keyHash } = deserializeAddress(await wallet.getChangeAddress());
    console.log("keyHash", keyHash);

    const nativeScript = {
      type: "sig",
      keyHash: keyHash
    };
    
    const forgingScript = ForgeScript.fromNativeScript(nativeScript);

    const tx = new Transaction({ initiator: wallet });
    console.log("tx", tx);
    
    tx.mintAsset(
      forgingScript,
      asset,
    );

    const unsignedTx = await tx.build();
    console.log("unsignedTx", unsignedTx);
    
    const signedTx = await wallet.signTx(unsignedTx);
    console.log("signedTx", signedTx);
    
    const txHash = await wallet.submitTx(signedTx);    
    console.log("txHash", txHash);
    
    const response = await waitForTx(txHash, isTest)
    console.log("response", response);

    const policyId = resolveNativeScriptHash(nativeScript);
    console.log("policyId", policyId);
    
    const assetNameHex = Buffer.from(_metadata.name, 'utf8').toString('hex');
    console.log("assetNameHex", assetNameHex);

    const unit = policyId + assetNameHex;
    console.log("unit", unit);
    
    return {
        response: response,
        transactionHash: txHash,
        policyId: policyId,
        assetNameHex: assetNameHex,
        unit: unit,
    };
}

// CIP-68
async function MintNFT68(toAddress, metadata, isTest) {
    let _metadata = metadata;
    let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;
    let adminAddr = await wallet.getChangeAddress();
    console.log("adminAddr", adminAddr);

    let balance = await getWalletBalance(adminAddr);
    console.log("balance", balance);
    
    if(typeof _metadata !== 'object') {
        _metadata = JSON.parse(_metadata);
    }

    if(!_metadata.name || !_metadata.name.trim()) {
        throw new Error("Metadata name is required");
    }

    if(!_metadata.attributes) {
        _metadata.attributes = [];
    }
    // _metadata.attributes.push({
    //                 "trait_type": "SIGNATURE",
    //                 "value": md5(JSON.stringify(_metadata) + process.env.NFT_SECRET)
    //             });

    // CIP-68 nft
    let userAsset = {   
      assetName: '222' + _metadata.name,
      assetQuantity: '1',
      metadata: _metadata,
    //   label: '222',
      recipient: toAddress
    };
    console.log("user asset", userAsset);

    let referenceAsset = {   
      assetName: '100' + _metadata.name,
      assetQuantity: '1',
      metadata: _metadata,
    //   label: '100',
      recipient: adminAddr
    };
    console.log("reference asset", referenceAsset);
    
    const { pubKeyHash: keyHash } = deserializeAddress(adminAddr);
    console.log("keyHash", keyHash);

    const nativeScript = {
      type: "sig",
      keyHash: keyHash
    };

    const policyId = resolveNativeScriptHash(nativeScript);
    console.log("policyId", policyId);
    
    const userAssetNameHex = Buffer.from('222' + _metadata.name, 'utf8').toString('hex');
    console.log("userAssetNameHex", userAssetNameHex);

    const userAssetUnit = policyId + userAssetNameHex;
    console.log("userAssetUnit", userAssetUnit);


    
    const forgingScript = ForgeScript.fromNativeScript(nativeScript);

    const userTx = new Transaction({ initiator: wallet });
    console.log("tx", userTx);
    
    userTx.mintAsset(
      forgingScript,
      userAsset,
    );

    // userTx.sendAssets(toAddress, [
    //     { unit: `${policyId}222${assetNameHex}`, quantity: '1' }
    // ]);

    const unsignedTx = await userTx.build();
    console.log("unsignedTx", unsignedTx);
    
    const signedTx = await wallet.signTx(unsignedTx);
    console.log("signedTx", signedTx);
    
    const txHash = await wallet.submitTx(signedTx);    
    console.log("txHash", txHash);
    
    const response = await waitForTx(txHash, isTest)
    console.log("response", response);

    
    await waitForTxConfirmed(txHash, 6*5, 10000);   // 5 minutes, every 10 seconds


    // mint reference asset
    const referenceTx = new Transaction({ initiator: wallet });
    console.log("reference tx", referenceTx);
    referenceTx.mintAsset(
        forgingScript,
        referenceAsset,
    );
    // referenceTx.sendAssets(adminAddr, [
    //     { unit: `${policyId}100${assetNameHex}`, quantity: '1' }
    // ]);
    const referenceUnsignedTx = await referenceTx.build();
    console.log("reference unsignedTx", referenceUnsignedTx);
    const referenceSignedTx = await wallet.signTx(referenceUnsignedTx);
    console.log("reference signedTx", referenceSignedTx);
    const referenceTxHash = await wallet.submitTx(referenceSignedTx);
    console.log("reference txHash", referenceTxHash);
    const referenceResponse = await waitForTx(referenceTxHash, isTest);
    console.log("reference response", referenceResponse);
    
    const referenceAssetNameHex = Buffer.from('100' + _metadata.name, 'utf8').toString('hex');
    console.log("referenceAssetNameHex", referenceAssetNameHex);

    const referenceAssetUnit = policyId + referenceAssetNameHex;
    console.log("referenceAssetUnit", referenceAssetUnit);

    return {
        response: response,
        transactionHash: txHash,
        policyId: policyId,
        assetNameHex: userAssetNameHex,
        unit: userAssetUnit,
        referenceTxHash: referenceTxHash,
        referenceAssetUnit: referenceAssetUnit
    };
}


// CIP-68
async function MintNFT68Reference(metadata, isTest) {
    console.log("MintNFT68Reference", metadata, isTest);
    
    let _metadata = metadata;
    let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;
    let adminAddr = await wallet.getChangeAddress();
    console.log("adminAddr", adminAddr);
    
    if(typeof _metadata !== 'object') {
        _metadata = JSON.parse(_metadata);
    }

    if(!_metadata.name || !_metadata.name.trim()) {
        throw new Error("Metadata name is required");
    }

    if(!_metadata.attributes) {
        _metadata.attributes = [];
    }
    // _metadata.attributes.push({
    //                 "trait_type": "SIGNATURE",
    //                 "value": md5(JSON.stringify(_metadata) + process.env.NFT_SECRET)
    //             });

    let referenceAsset = {   
      assetName: '100' + _metadata.name,
      assetQuantity: '1',
      metadata: _metadata,
    //   label: '100',
      recipient: adminAddr
    };
    console.log("reference asset", referenceAsset);
    
    const { pubKeyHash: keyHash } = deserializeAddress(adminAddr);
    console.log("keyHash", keyHash);

    const nativeScript = {
      type: "sig",
      keyHash: keyHash
    };

    const policyId = resolveNativeScriptHash(nativeScript);
    console.log("policyId", policyId);
    
    
    const forgingScript = ForgeScript.fromNativeScript(nativeScript);


    // mint reference asset
    const referenceTx = new Transaction({ initiator: wallet });
    console.log("reference tx", referenceTx);
    referenceTx.mintAsset(
        forgingScript,
        referenceAsset,
    );
    // referenceTx.sendAssets(adminAddr, [
    //     { unit: `${policyId}100${assetNameHex}`, quantity: '1' }
    // ]);
    const referenceUnsignedTx = await referenceTx.build();
    console.log("reference unsignedTx", referenceUnsignedTx);
    const referenceSignedTx = await wallet.signTx(referenceUnsignedTx);
    console.log("reference signedTx", referenceSignedTx);
    const referenceTxHash = await wallet.submitTx(referenceSignedTx);
    console.log("reference txHash", referenceTxHash);
    const referenceResponse = await waitForTx(referenceTxHash, isTest);
    console.log("reference response", referenceResponse);
    
    const referenceAssetNameHex = Buffer.from('100' + _metadata.name, 'utf8').toString('hex');
    console.log("referenceAssetNameHex", referenceAssetNameHex);

    const referenceAssetUnit = policyId + referenceAssetNameHex;
    console.log("referenceAssetUnit", referenceAssetUnit);

    return {
        response: referenceResponse,
        policyId: policyId,
        transactionHash: referenceTxHash,
        referenceAssetUnit: referenceAssetUnit
    };
}

async function BurnNFT(unit, isTest) {
    console.log("BurnNFT", unit, isTest);
    
    let ownerAddress = await CheckMembershipOwnerOf(unit, isTest);
    console.log("ownerAddress", ownerAddress);
    
    let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;
    console.log("admin wallet address", await wallet.getChangeAddress());

    const policyId = unit.slice(0, 56);
    console.log("policyId", policyId);
    
    const assetNameHex = unit.slice(56);
    console.log("assetNameHex", assetNameHex);
    
    const assetName = Buffer.from(assetNameHex, 'hex').toString('utf8');
    console.log("assetName", assetName);

    const { pubKeyHash: keyHash } = deserializeAddress(await wallet.getChangeAddress());
    console.log("keyHash", keyHash);
    
    const nativeScript = {
        type: "sig",
        keyHash: keyHash
    };

    // ⚠️ Validate derived policyId matches expected policyId
    const derivedPolicyId = resolveNativeScriptHash(nativeScript);
    if (derivedPolicyId !== policyId) {
        throw new Error(`Mismatch in policyId: expected ${policyId}, got ${derivedPolicyId}`);
    }

    const forgingScript = ForgeScript.fromNativeScript(nativeScript);

    const tx = new Transaction({ initiator: wallet });

    tx.burnAsset(
        forgingScript,
        {
            unit: unit,
            quantity: 1
            // assetName: assetName,
            // assetQuantity: '-1',
            //label: '721',
            //recipient: ownerAddress //await wallet.getChangeAddress()
        }
    );

    const unsignedTx = await tx.build();
    const signedTx = await wallet.signTx(unsignedTx);
    const txHash = await wallet.submitTx(signedTx);
    console.log("txHash", txHash);
    
    const response = await waitForTx(txHash, isTest);
    console.log("response", response);
    

    return {
        response,
        transactionHash: txHash,
        policyId: derivedPolicyId,
        assetName,
        unit
    };
}


async function BurnNFT68Reference(unit, isTest) {
    console.log("BurnNFTReference", unit, isTest);
    
    let wallet = isTest ? adminCardanoWalletTest : adminCardanoWallet;
    console.log("admin wallet address", await wallet.getChangeAddress());

    const policyId = unit.slice(0, 56);
    console.log("policyId", policyId);
    
    const assetNameHex = unit.slice(56);
    console.log("assetNameHex", assetNameHex);
    
    const assetName = Buffer.from(assetNameHex, 'hex').toString('utf8');
    console.log("assetName", assetName);

    const referenceAssetName = assetName.replace("222", "100");
    const referenceAssetNameHex = Buffer.from(referenceAssetName, 'utf8').toString('hex');
    console.log("referenceAssetNameHex", referenceAssetNameHex);

    const referenceUnit = policyId + referenceAssetNameHex;
    console.log("referenceUnit", referenceUnit);
    
    const { pubKeyHash: keyHash } = deserializeAddress(await wallet.getChangeAddress());
    console.log("keyHash", keyHash);
    
    const nativeScript = {
        type: "sig",
        keyHash: keyHash
    };

    // ⚠️ Validate derived policyId matches expected policyId
    const derivedPolicyId = resolveNativeScriptHash(nativeScript);
    if (derivedPolicyId !== policyId) {
        throw new Error(`Mismatch in policyId: expected ${policyId}, got ${derivedPolicyId}`);
    }

    const forgingScript = ForgeScript.fromNativeScript(nativeScript);

    const tx = new Transaction({ initiator: wallet });

    tx.burnAsset(
        forgingScript,
        {
            unit: referenceUnit,
            quantity: 1
            // assetName: assetName,
            // assetQuantity: '-1',
            //label: '721',
            //recipient: ownerAddress //await wallet.getChangeAddress()
        }
    );

    const unsignedTx = await tx.build();
    const signedTx = await wallet.signTx(unsignedTx);
    const txHash = await wallet.submitTx(signedTx);
    console.log("txHash", txHash);
    
    const response = await waitForTx(txHash, isTest);
    console.log("response", response);
    

    return {
        response,
        transactionHash: txHash,
        policyId: derivedPolicyId,
        assetName,
        unit
    };
}

async function isTxConfirmed(txHash) {
  try {
    // preview.koios.rest works only for the preview testnet.
	// For mainnet, use https://api.koios.rest/api/v1/tx_status.
	// If you need full metadata or UTxOs, Koios also supports that.
    //const response = await axios.post('https://preview.koios.rest/api/v1/tx_status', {
    const response = await axios.post('https://api.koios.rest/api/v1/tx_status', {
      _tx_hashes: [txHash]
    });

    const confirmations = response.data[0]?.num_confirmations ?? 0;
    console.log("confirmations", confirmations);
    
    return confirmations >= 2;
  } catch (err) {
    console.error('Error checking tx status:', err.message);
    return false;
  }
}

async function waitForTxConfirmed(txHash, retries = 30, intervalMs = 5000) {
  for (let i = 0; i < retries; i++) {
    const confirmed = await isTxConfirmed(txHash);
    if (confirmed) {
      console.log(`✅ TX ${txHash} confirmed.`);
      return true;
    }
    console.log(`⏳ Waiting for confirmation... (${i + 1}/${retries})`);
    await new Promise(res => setTimeout(res, intervalMs));
  }
  throw new Error(`❌ TX ${txHash} not confirmed after ${retries * intervalMs / 1000} seconds.`);
}