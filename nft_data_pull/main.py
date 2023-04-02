import os
import json
import requests
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


def hex_to_int(hex) -> int:
    try:
        return int(hex, 16)
    except:
        return hex

def pull_all_contracts():
    contracts = []

    for page_n in range(1000):
        api_query = f"{celo_base_api_url}/?module=contract&action=listcontracts&filter=verified&page={page_n}"
        result = requests.get(api_query).json()
        #with open('./tests/samples/contracts.json', 'w') as f:
        #    json.dump(result, f, indent=4)
        df = pd.json_normalize(result["result"])
        if df.empty:
            break
        else:
            contracts.append(df)

    df_contracts = pd.concat(contracts)
    df_contracts.to_csv("./output/staging/contracts.csv", index=False)

def get_active_nft_collections():
    df_contracts = pd.read_csv("./output/staging/contracts.csv")
    df_filtered = df_contracts[
        df_contracts["ABI"].str.contains("tokenURI", case=False) & df_contracts["ABI"].str.contains("totalSupply", case=False)
    ]

    nft_collection_info_list = []

    for index, row in df_filtered.iterrows():
        try:
            contract_name = str(row["ContractName"])

            # filter out based on contract name
            if "test" in contract_name.lower() or "smartcontract" in contract_name.lower():
                continue

            contract_address = w3.to_checksum_address(row["Address"])
            contract_abi = row["ABI"]

            contract_instance = w3.eth.contract(address=contract_address, abi=contract_abi)

            # filter out if contract is paused
            if "paused" in contract_abi:
                is_paused = contract_instance.functions.paused().call()
                if is_paused == True:
                    continue

            # filter out if collection is less than 10 nfts
            total_supply = contract_instance.functions.totalSupply().call()
            if total_supply < 10:
                continue
            
            # pull token symbol
            if "symbol" in contract_abi:
                symbol = str(contract_instance.functions.symbol().call())
            else:
                symbol = ""
            
            # filter out basd on symbol name
            if "test" in symbol.lower():
                continue

            # pull owner
            if "owner" in contract_abi:
                owner = contract_instance.functions.owner().call()
            else:
                owner = ""
            
            nft_collection_info_row = {
                "chain" : "Celo",
                "collection_name" : contract_name,
                "collection_slug" : contract_name.lower(),
                "contract_address" : contract_address,
                "created_date" : "TODO",
                "deploy_block_number" : "TODO",
                "deploy_transaction_hash" : "TODO",
                "description" : "TODO",
                "owner" : owner,
                "protocol_slug" : "TODO",
                "standard" : "TODO",
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
    #token_uri = contract_instance.functions.tokenURI(1).call()
    pass

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
    contract_address = "0x179513e0fa9B5AD964405B01194105A2d8e0c2df" # just test
    nft_tranfer_list = []

    """
    api_query = f"{celo_base_api_url}/?module=token&action=tokentx&contractaddress={contract_address}&fromBlock=0&toBlock=latest"
    result = requests.get(api_query).json()
    with open('./tests/samples/transfers.json', 'w') as f:
        json.dump(result, f, indent=4)
    df_transfers = pd.json_normalize(result["result"])
    df_transfers.to_csv("./output/staging/transfers.csv", index=False)
    """

    df_transfers = pd.read_csv("./output/staging/transfers.csv")

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

    df_nft_tranfer = pd.DataFrame(nft_tranfer_list, columns=config.nft_transfers_columns)
    df_nft_tranfer.to_csv("./output/nft_transfers.csv", index=False)
        

##########
#pull_all_contracts()
#get_active_nft_collections()
#pull_nft_transactions()
pull_nft_transfers()
