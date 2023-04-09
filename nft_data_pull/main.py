import os
import sys
import ast
import json
import requests
import mimetypes
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from web3 import Web3

import config

# load env vars
load_dotenv()
infura_url = os.getenv("INFURA_URL")

# global vars
w3 = Web3(Web3.HTTPProvider(infura_url))
celo_base_api_url = "https://explorer.celo.org/mainnet/api"


def hex_to_int(hex):
    try:
        return int(hex, 16)
    except:
        return hex

def pull_nft_contracts():
    nft_contracts_list = []

    for page_n in range(1, 1000):
        api_query = f"{celo_base_api_url}/?module=contract&action=listcontracts&filter=verified&page={page_n}"
        contract = requests.get(api_query).json()
        #with open('./tests/samples/contracts.json', 'w') as f:
        #    json.dump(result, f, indent=4)
        df_contract = pd.json_normalize(contract["result"])
        if df_contract.empty:
            break
        else:
            # pre-filter contracts to only include ones that:
            # - have tokenURI and totalSupply methods and..
            df_filtered = df_contract[
                df_contract["ABI"].str.contains("tokenURI", case=False) & 
                df_contract["ABI"].str.contains("totalSupply", case=False)
            ]

            # - .. contract name doesn't contain "SmartContract" or "Test" strings
            df_filtered = df_filtered[
                df_filtered["ContractName"].str.contains("SmartContract|test", case=False) == False
            ]
            
            nft_contracts_list.append(df_filtered)

    df_contracts = pd.concat(nft_contracts_list)
    df_contracts.to_csv("./output/staging/nft_contracts.csv", index=False)

def get_active_nft_collections():
    df_nft_contracts = pd.read_csv("./output/staging/nft_contracts.csv")
    nft_collection_info_list = []

    for _, row in df_nft_contracts.iterrows():
        try:
            contract_name = str(row["ContractName"])
            contract_address = w3.to_checksum_address(row["Address"])
            contract_abi = row["ABI"]

            contract_instance = w3.eth.contract(address=contract_address, abi=contract_abi)

            # filter out if contract is paused
            if "paused" in contract_abi:
                is_paused = contract_instance.functions.paused().call()
                if is_paused is True:
                    continue
            
            api_query = f"{celo_base_api_url}/?module=token&action=getToken&contractaddress={contract_address}"
            token = requests.get(api_query).json()

            # skip if status not 1 : OK
            if token["status"] != "1":
                continue

            symbol = token["result"]["symbol"]
            total_supply = int(token["result"]["totalSupply"])
            token_type = token["result"]["type"]

            # double check total_supply
            if total_supply == 0:
                total_supply = contract_instance.functions.totalSupply().call()
            # filter out if collection is less than 10 nfts
            if total_supply < 10:
                continue
            
            # if blank - pull token symbol
            if symbol == "" and "symbol" in contract_abi:
                symbol = str(contract_instance.functions.symbol().call())
            
            # filter out basd on symbol name
            if "test" in symbol.lower():
                continue
            
            api_query = f"{celo_base_api_url}/?module=account&action=txlist&address={contract_address}&sort=asc&start_block=0&page=1&offset=1"
            first_tx = requests.get(api_query).json()
            #with open('./tests/samples/first_tx.json', 'w') as f:
            #    json.dump(result, f, indent=4)
            timestamp = datetime.fromtimestamp(int(first_tx["result"][0]["timeStamp"])).replace(microsecond=0).isoformat()
            
            # pull owner
            if "owner" in contract_abi:
                owner = contract_instance.functions.owner().call()
            if owner == "":
                owner = first_tx["result"][0]["from"]
            
            nft_collection_info_row = {
                "chain" : "Celo",
                "collection_name" : contract_name,
                "collection_slug" : contract_name.lower(),
                "contract_address" : contract_address,
                "created_date" : datetime.fromisoformat(timestamp).date(),
                "deploy_block_number" : first_tx["result"][0]["blockNumber"],
                "deploy_transaction_hash" : first_tx["result"][0]["hash"],
                "owner" : owner,
                "standard" : token_type,
                "symbol" : symbol,
                "total_supply" : total_supply
            }
            nft_collection_info_list.append(nft_collection_info_row)

        except:
            print("exception for:", contract_address)
            continue

    df_nft_collection_info = pd.DataFrame(nft_collection_info_list, columns=config.nft_collection_info_columns)
    df_nft_collection_info.to_csv("./output/nft_collection_info.csv", index=False)

def pull_nft_info():
    df_nft_collection_info = pd.read_csv("./output/nft_collection_info.csv")
    nft_info_list = []

    for _, row in df_nft_collection_info.iterrows():
        try:
            collection_name = str(row["collection_name"])
            collection_slug = str(row["collection_slug"])
            contract_address = w3.to_checksum_address(row["contract_address"])

            print(collection_name, ":", contract_address, "start..")

            api_query = f"{celo_base_api_url}/?module=contract&action=getabi&address={contract_address}"
            contract_abi = requests.get(api_query).json()["result"]
            contract_instance = w3.eth.contract(address=contract_address, abi=contract_abi)
            total_supply = int(row["total_supply"])
            err_count = 0

            # get token_uri template for last token
            token_uri_url = contract_instance.functions.tokenURI(total_supply).call()

            for token_id in range(1, total_supply + 1):
                try:
                    token_uri = str(token_uri_url).replace("/" + str(total_supply), "/" + str(token_id))
                    if "/Qm" in token_uri:
                        token_uri = "https://ipfs.io/ipfs" + token_uri[str(token_uri).find("/Qm"):]
                    nft_metadata = requests.get(token_uri).json()

                    token_description = ""
                    if "description" in nft_metadata:
                        token_description = nft_metadata["description"]

                    nft_info_row = {
                        "chain": "Celo",
                        "collection_contract_address": contract_address,
                        "collection_name": collection_name,
                        "collection_slug": collection_slug,
                        "description": token_description,
                        "image_mime_type": mimetypes.guess_type(nft_metadata["image"])[0],
                        "image_url": nft_metadata["image"],
                        "metadata": nft_metadata,
                        "metadata_uri": token_uri,
                        "nft_token_id": token_id,
                        "token_name": nft_metadata["name"]
                    }
                    nft_info_list.append(nft_info_row)

                    if token_id % 100 == 0 or token_id == total_supply:
                        print("..", contract_address, "progress :", token_id)
                except:
                    err_count += 1
                    err_msg = f"{sys.exc_info()[0]}, {sys.exc_info()[1]}, line: {sys.exc_info()[2].tb_lineno}"
                    print("nft_info token error for", contract_address, "token_id:", token_id, "\n", err_msg)
                    if err_count < 10:
                        continue
                    else:
                        break

            print(collection_name, ":", contract_address, "end..")
        except:
            err_msg = f"{sys.exc_info()[0]}, {sys.exc_info()[1]}, line: {sys.exc_info()[2].tb_lineno}"
            print("nft_info error for", collection_name, ":", contract_address, "\n", err_msg)
            continue

    df_nft_tranfer = pd.DataFrame(nft_info_list, columns=config.nft_info_columns)
    #df_nft_tranfer.to_csv("./output/nft_info.csv", index=False)
    df_nft_tranfer.to_csv("./output/nft_info.csv", mode="a", index=False, header=False)

def pull_nft_token_attributes():
    df_nft_info = pd.read_csv("./output/nft_info.csv")
    nft_token_attributes_list = []

    for _, row in df_nft_info.iterrows():
        try:
            collection_contract_address = row["collection_contract_address"]
            collection_slug = row["collection_slug"]
            token_id = row["nft_token_id"]

            json_md = json.dumps(ast.literal_eval(row["metadata"]))
            dict_md = json.loads(json_md)
            
            if "attributes" in dict_md:
                df_md = pd.json_normalize(dict_md["attributes"]).rename(
                    index=str,
                    columns={"trait_type": "attribute_key", "value": "attribute_value"}
                )
                df_md["attribute_type"] = df_md["attribute_value"].apply(lambda s: type(s).__name__)
                df_md["nft_token_id"] = token_id
                df_md["chain"] = "Celo"
                df_md["collection_contract_address"] = collection_contract_address
                df_md["collection_slug"] = collection_slug

                nft_token_attributes_list.append(df_md)

                if token_id % 100 == 0:
                    print("..", collection_contract_address, "progress :", token_id)
        except:
            err_msg = f"{sys.exc_info()[0]}, {sys.exc_info()[1]}, line: {sys.exc_info()[2].tb_lineno}"
            print("nft_token_attributes error for", collection_contract_address, "tokenId:", token_id, "\n", err_msg)
            continue

    df_nft_token_attributes = pd.concat(nft_token_attributes_list)
    df_nft_token_attributes[["created_at", "updated_at"]] = ""
    df_nft_token_attributes.to_csv("./output/nft_token_attributes.csv", index=False)

def pull_nft_transactions():
    contract_address = "0x179513e0fa9B5AD964405B01194105A2d8e0c2df" # just test

    transactions = []
    nft_tx_list = []

    """
    for page_n in range(1, 1000):
        api_query = f"{celo_base_api_url}/?module=account&action=txlist&address={contract_address}&sort=asc&page={page_n}"
        result = requests.get(api_query).json()
        with open('./tests/samples/transactions.json', 'w') as f:
            json.dump(result, f, indent=4)
        df = pd.json_normalize(result["result"])
        if df.empty:
            break
        else:
            transactions.append(df)
        break

    df_txs = pd.concat(transactions)
    df_txs.to_csv("./output/staging/transactions.csv", index=False)
    """

    df_txs = pd.read_csv("./output/staging/transactions.csv")

    for _, row in df_txs.iterrows():
        timestamp = datetime.fromtimestamp(hex_to_int(row["timeStamp"])).replace(microsecond=0).isoformat()
        
        nft_tx_row = {
            "amount": "",
            "amount_currency": "",
            "amount_currency_contract_address": "",
            "block_date": datetime.fromisoformat(timestamp).date(),
            "block_number": hex_to_int(row["blockNumber"]),
            "block_timestamp": timestamp,
            "block_timestampf_value - latest_time_after_yesterday": "",
            "buyer_address": "",
            "chain": "Celo",
            "collection_contract_address": contract_address,
            "collection_slug": "",
            "internal_index": "",
            "log_index": "",
            "marketplace_contract_address": "",
            "marketplace_slug": "",
            "nft_token_id": "",
            "number_of_nft_token_id": "",
            "platform_fee_rate": "",
            "platform_fees_amount": "",
            "platform_fees_value": "",
            "royalty_amount": "",
            "royalty_rate": "",
            "royalty_value": "",
            "seller_address": "",
            "trade_type": "",
            "transaction_hash": "",
            "value": "",
            "value_currency": ""
        }

def pull_nft_transfers():
    df_nft_collection_info = pd.read_csv("./output/nft_collection_info.csv")
    nft_tranfer_list = []

    for _, row in df_nft_collection_info.iterrows():
        try:
            collection_name = str(row["collection_name"])
            contract_address = w3.to_checksum_address(row["contract_address"])

            print(collection_name, ":", contract_address, "start..")

            api_query = f"{celo_base_api_url}/?module=token&action=tokentx&contractaddress={contract_address}&fromBlock=0&toBlock=latest"
            transfers = requests.get(api_query).json()
            #with open('./tests/samples/transfers.json', 'w') as f:
            #    json.dump(transfers, f, indent=4)
            df_transfers = pd.json_normalize(transfers["result"])

            for _, row in df_transfers.iterrows():
                timestamp = datetime.fromtimestamp(hex_to_int(row["timeStamp"])).replace(microsecond=0).isoformat()

                topics = [str(item).strip("'") for item in row["topics"].strip("[]").split(", ")]
                method_id = topics[0][:10]
                if len(topics) == 4:
                    token_id = hex_to_int(topics[3])
                else:
                    token_id = 0
                
                nft_tranfer_row = {
                    "amount_raw": token_id,
                    "block_date": datetime.fromisoformat(timestamp).date(),
                    "block_number": hex_to_int(row["blockNumber"]),
                    "block_timestamp": timestamp,
                    "chain": "Celo",
                    "collection_contract_address": contract_address,
                    "from_address": row["fromAddressHash"],
                    "internal_index": hex_to_int(row["transactionIndex"]),
                    "log_index": hex_to_int(row["logIndex"]),
                    "nft_token_id": token_id,
                    "to_address": row["fromAddressHash"],
                    "transaction_hash": row["transactionHash"],
                    "transfer_type": config.method_types[method_id]
                }
                nft_tranfer_list.append(nft_tranfer_row)

            print(collection_name, ":", contract_address, "end..")
        except:
            err_msg = f"{sys.exc_info()[0]}, {sys.exc_info()[1]}, line: {sys.exc_info()[2].tb_lineno}"
            print("nft_transfers error for", collection_name, ":", contract_address, "tokenId:", token_id, "\n", err_msg)
            continue

    df_nft_tranfer = pd.DataFrame(nft_tranfer_list, columns=config.nft_transfers_columns)
    df_nft_tranfer.to_csv("./output/nft_transfers.csv", index=False)
        

##########
#pull_nft_contracts()
#get_active_nft_collections()
#pull_nft_transactions()
#pull_nft_transfers()
pull_nft_info()
#pull_nft_token_attributes()
