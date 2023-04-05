import os
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

    for _, row in df_filtered.iterrows():
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
                if is_paused is True:
                    continue
            
            api_query = f"{celo_base_api_url}/?module=token&action=getToken&contractaddress={contract_address}"
            token = requests.get(api_query).json()

            # skip if not status = 1 : OK
            if token["status"] != "1":
                continue

            symbol = token["result"]["symbol"]
            total_supply = int(token["result"]["totalSupply"])
            token_type = token["result"]["type"]

            # filter out if collection is less than 10 nfts
            if total_supply == 0:
                total_supply = contract_instance.functions.totalSupply().call()
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
    nft_info_list = []
    contract_address = w3.to_checksum_address("0xAc80c3c8b122DB4DcC3C351ca93aC7E0927C605d") # just test
    contract_abi = '[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Paused","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Unpaused","type":"event"},{"inputs":[{"internalType":"address","name":"_addressToWhitelist","type":"address"}],"name":"addUser","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"baseExtension","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"baseURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"closeWhitelist","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"cost","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isWhitelist","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxMintAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_to","type":"address"},{"internalType":"uint16","name":"_mintAmount","type":"uint16"}],"name":"mint","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"onlyWhitelist","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"openWhitelist","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"_newBaseExtension","type":"string"}],"name":"setBaseExtension","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"_newBaseURI","type":"string"}],"name":"setBaseURI","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_newCost","type":"uint256"}],"name":"setCost","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_newmaxMintAmount","type":"uint256"}],"name":"setMaxMintAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenOfOwnerByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"unpause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"}],"name":"walletOfOwner","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"withdraw","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"contract IERC20","name":"token","type":"address"}],"name":"withdrawERC20","outputs":[],"stateMutability":"nonpayable","type":"function"}]'
    contract_instance = w3.eth.contract(address=contract_address, abi=contract_abi)
    
    for token_id in range(1,10):
        token_uri = contract_instance.functions.tokenURI(token_id).call()
        nft_metadata = requests.get(token_uri).json()

        nft_info_row = {
            "animation_url": "",
            "chain": "Celo",
            "collection_contract_address": contract_address,
            "collection_name": "TODO",
            "collection_slug": "TODO",
            "description": "",
            "image_mime_type": mimetypes.guess_type(nft_metadata["image"])[0],
            "image_url": nft_metadata["image"],
            "internet_mime_type": "",
            "metadata": nft_metadata,
            "metadata_uri": token_uri,
            "mint_address": "",
            "mint_block_timestamp": "",
            "mint_transaction_hash": "",
            "nft_token_id": token_id,
            "token_name": nft_metadata["name"]
        }
        nft_info_list.append(nft_info_row)

    df_nft_tranfer = pd.DataFrame(nft_info_list, columns=config.nft_info_columns)
    df_nft_tranfer.to_csv("./output/nft_info.csv", index=False, )

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
#pull_nft_transfers()
pull_nft_info()
