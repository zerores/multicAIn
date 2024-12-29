import pandas as pd
import requests
import time
import json
from tqdm import tqdm
from collections import defaultdict
import pickle
import os


SOL_THRESHOLD = 0.2  
TRANSACTION_LIMIT = 1000  
INTERACTION_THRESHOLD = 2  
SAVE_INTERVAL = 10  
MAX_RETRIES = 3  
RETRY_DELAY = 5  

# Solana RPC endpoint
url = "https://api.mainnet-beta.solana.com"


CHECKPOINT_FILE = 'analysis_checkpoint.pkl'
TEMP_RESULTS_FILE = 'temp_results.xlsx'

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            return pickle.load(f)
    return {
        'processed_index': 0,
        'address_graph': defaultdict(set),
        'interaction_count': defaultdict(int),
        'df': None
    }

def save_checkpoint(checkpoint_data):
    with open(CHECKPOINT_FILE, 'wb') as f:
        pickle.dump(checkpoint_data, f)

    if checkpoint_data['df'] is not None:
        checkpoint_data['df'].to_excel(TEMP_RESULTS_FILE, index=False)
    print(f"Checkpoint saved at index {checkpoint_data['processed_index']}")

def make_request(method, url, headers, data, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=data, timeout=30)
            if response.status_code == 200:
                return response
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                print(f"Error after {max_retries} attempts: {str(e)}")
                return None
            print(f"Request failed, retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    return None

def get_balance(address):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [address]
    }
    
    response = make_request("POST", url, headers, data)
    if response and response.status_code == 200:
        result = response.json()
        if 'result' in result:
            return result['result']['value'] / 1_000_000_000
    return None

def get_recent_transactions(address):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            address,
            {"limit": TRANSACTION_LIMIT}
        ]
    }
    
    response = make_request("POST", url, headers, data)
    if response and response.status_code == 200:
        result = response.json()
        if 'result' in result:
            return [tx['signature'] for tx in result['result']]
    return []

def get_transaction_details(signature):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    }
    
    response = make_request("POST", url, headers, data)
    if response and response.status_code == 200:
        result = response.json()
        if 'result' in result and result['result']:
            return result['result']
    return None


try:

    checkpoint = load_checkpoint()
    if checkpoint['df'] is None:
        df = pd.read_excel('multicAIn capital DAOs.xlsx', header=None)
        df['Balance'] = ''
        df['Whitelist Recommendation'] = ''
        df['Related Addresses'] = ''
        df['Risk Score'] = ''
    else:
        df = checkpoint['df']
        print(f"Resuming from index {checkpoint['processed_index']}")

    address_graph = checkpoint['address_graph']
    interaction_count = checkpoint['interaction_count']
    

    valid_addresses = []
    current_cluster = None
    cluster_addresses = defaultdict(list)

    for index, row in df.iterrows():
        address = row[0]
        if pd.isna(address):
            continue
        if str(address).startswith('Cluster'):
            current_cluster = address.strip(':')
            continue
        if current_cluster and not pd.isna(address):
            valid_addresses.append(address)
            cluster_addresses[current_cluster].append(address)


    start_index = checkpoint['processed_index']
    
    print("Analyzing transaction relationships...")
    for i, address in enumerate(tqdm(valid_addresses[start_index:], initial=start_index, total=len(valid_addresses))):
        try:
            balance = get_balance(address)
            if balance is not None:
                df.loc[df[0] == address, 'Balance'] = balance
            
        
            transactions = get_recent_transactions(address)
            for tx_sig in transactions:
                tx_details = get_transaction_details(tx_sig)
                if tx_details and 'message' in tx_details['transaction']:
                    accounts = set()
                    for account in tx_details['transaction']['message']['accountKeys']:
                        if account['pubkey'] in valid_addresses:
                            accounts.add(account['pubkey'])
                    
                    if len(accounts) > 1:
                        for acc1 in accounts:
                            for acc2 in accounts:
                                if acc1 != acc2:
                                    address_graph[acc1].add(acc2)
                                    interaction_count[(acc1, acc2)] += 1
                
                time.sleep(0.1)

         
            if (i + 1) % SAVE_INTERVAL == 0:
                checkpoint['processed_index'] = start_index + i + 1
                checkpoint['address_graph'] = address_graph
                checkpoint['interaction_count'] = interaction_count
                checkpoint['df'] = df
                save_checkpoint(checkpoint)

        except Exception as e:
            print(f"Error processing address {address}: {str(e)}")
       
            checkpoint['processed_index'] = start_index + i
            checkpoint['address_graph'] = address_graph
            checkpoint['interaction_count'] = interaction_count
            checkpoint['df'] = df
            save_checkpoint(checkpoint)
            raise e


    address_groups = []
    processed_addresses = set()

    for address in valid_addresses:
        if address in processed_addresses:
            continue
        
        related = set([address])
        to_process = set([address])
        
        while to_process:
            current = to_process.pop()
            for related_address in address_graph[current]:
                if interaction_count[(current, related_address)] >= INTERACTION_THRESHOLD:
                    if related_address not in related:
                        related.add(related_address)
                        to_process.add(related_address)
        
        if len(related) > 1:
            address_groups.append(related)
            processed_addresses.update(related)


    for index, row in df.iterrows():
        address = row[0]
        if pd.isna(address) or str(address).startswith('Cluster'):
            continue
        
        balance = row['Balance']
        if isinstance(balance, (int, float)):
            if balance >= SOL_THRESHOLD:
                df.at[index, 'Whitelist Recommendation'] = 'Yes'
            else:
                df.at[index, 'Whitelist Recommendation'] = 'No'
            
  
            related_addresses = []
            risk_score = 0
            
            for group in address_groups:
                if address in group:
                    related_addresses = list(group - {address})
                    risk_score = len(group) - 1
                    break
            
            df.at[index, 'Related Addresses'] = ', '.join(related_addresses) if related_addresses else 'None'
            df.at[index, 'Risk Score'] = risk_score
            
     
            if risk_score >= 2:
                df.at[index, 'Whitelist Recommendation'] = 'No (High Risk)'


    output_file = 'multicAIn capital DAOs_with_relationship_analysis2.xlsx'
    df.to_excel(output_file, index=False, header=True)


    print("\nAnalysis Summary:")
    print(f"Total address groups found: {len(address_groups)}")
    for i, group in enumerate(address_groups, 1):
        print(f"\nGroup {i} (Size: {len(group)}):")
        for addr in group:
            balance = df.loc[df[0] == addr, 'Balance'].iloc[0]
            print(f"  Address: {addr}, Balance: {balance:.3f} SOL")

    print(f"\nResults saved to {output_file}")

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    if os.path.exists(TEMP_RESULTS_FILE):
        os.remove(TEMP_RESULTS_FILE)

except Exception as e:
    print(f"An error occurred: {str(e)}")
    print("Progress has been saved. You can resume later by running the script again.")