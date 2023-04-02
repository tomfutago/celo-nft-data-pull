import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

import config

# load env vars
load_dotenv()
infura_url = os.getenv("INFURA_URL")

# global vars
w3 = Web3(Web3.HTTPProvider(infura_url))
celo_base_api_url = "https://explorer.celo.org/mainnet/api"

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
    df_contracts.to_csv("./tests/samples/contracts.csv", index=False)

def get_active_nft_collections():
    df_contracts = pd.read_csv("./tests/samples/contracts.csv")
    df_filtered = df_contracts[
        df_contracts["ABI"].str.contains("tokenURI", case=False) & df_contracts["ABI"].str.contains("totalSupply", case=False)
    ]

    nft_collection_info_list = []

    for index, row in df_filtered.iterrows():
        try:
            contract_name = row["ContractName"]

            # filter out based on contract name
            if "test" in contract_name or "SmartContract" in contract_name:
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

            # pull owner
            if "owner" in contract_abi:
                owner = contract_instance.functions.owner().call()
            else:
                owner = ""
            
            # pull token symbol
            if "symbol" in contract_abi:
                symbol = contract_instance.functions.symbol().call()
            else:
                symbol = ""

            #token_uri = contract_instance.functions.tokenURI(1).call()
            
            nft_collection_info_row = {
                "chain" : "Celo",
                "collection_name" : contract_name,
                "collection_slug" : str(contract_name).lower(),
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

##########
#pull_all_contracts()
get_active_nft_collections()
