import pandas as pd
import aiohttp
import asyncio
import time
import json
import nest_asyncio
from tqdm import tqdm
from collections import defaultdict
import itertools
from typing import List, Dict, Set
import random
import pickle
from datetime import datetime


nest_asyncio.apply()

SOL_THRESHOLD = 0.5
TRANSACTION_LIMIT = 100  
INTERACTION_THRESHOLD = 2
BATCH_SIZE = 1
MAX_CONCURRENT_REQUESTS = 1
MAX_RETRIES = 5
RETRY_DELAY = 2
RATE_LIMIT_DELAY = 2
WAIT_TIME = 1  


RPC_ENDPOINTS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-api.projectserum.com",
    "https://rpc.ankr.com/solana"
]


global url
url = RPC_ENDPOINTS[0] 

CACHE_FILE = 'solana_data_cache.pkl'


try:
    with open(CACHE_FILE, 'rb') as f:
        cache = pickle.load(f)
    print(f"Loaded cache with {len(cache)} entries")
except FileNotFoundError:
    cache = {
        'balances': {},
        'transactions': {},
        'transaction_details': {}
    }
    print("Created new cache")

def save_cache():
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)
    print(f"Saved cache with {len(cache['balances'])} balances, "
          f"{len(cache['transactions'])} transaction lists, "
          f"{len(cache['transaction_details'])} transaction details")


# async def retry_request(session, method: str, params_list: List, batch_name: str = "") -> List:
#     global url
#     for attempt in range(MAX_RETRIES):
     
#         try:
#             await asyncio.sleep(0.5)
            
#             async with session.post(url, json=[{
#                 "jsonrpc": "2.0",
#                 "id": i,
#                 "method": method,
#                 "params": params
#             } for i, params in enumerate(params_list)]) as response:
                
#                 if response.status == 200:
#                     return await response.json()
#                 elif response.status == 429:  # Rate limit
#                    
#                     current_index = RPC_ENDPOINTS.index(url)
#                     url = RPC_ENDPOINTS[(current_index + 1) % len(RPC_ENDPOINTS)]
                    
#                    
#                     wait_time = WAIT_TIME + (attempt * 1)  
#                     print(f"Rate limit hit in {batch_name}, switching to {url} and waiting {wait_time}s...")
#                     await asyncio.sleep(wait_time)
#                     continue
#                 else:
#                  
#                     current_index = RPC_ENDPOINTS.index(url)
#                     url = RPC_ENDPOINTS[(current_index + 1) % len(RPC_ENDPOINTS)]
#                     print(f"Error {response.status}, switching to {url}")
#                     await asyncio.sleep(WAIT_TIME)
#                     continue
                    
#         except Exception as e:
#             print(f"Error in {batch_name}: {str(e)}")
#             current_index = RPC_ENDPOINTS.index(url)
#             url = RPC_ENDPOINTS[(current_index + 1) % len(RPC_ENDPOINTS)]
#             await asyncio.sleep(WAIT_TIME)
#             continue
            
#     return []

async def retry_request(session, method: str, params_list: List, batch_name: str = "") -> List:
    global url
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(0.5)
            
            async with session.post(url, json=[{
                "jsonrpc": "2.0",
                "id": i,
                "method": method,
                "params": params
            } for i, params in enumerate(params_list)]) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limit
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"Rate limit hit in {batch_name}, waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                 
                    current_index = RPC_ENDPOINTS.index(url)
                    url = RPC_ENDPOINTS[(current_index + 1) % len(RPC_ENDPOINTS)]
                    print(f"Switching to {url}")
                    await asyncio.sleep(1)
                    continue
        except Exception as e:
            print(f"Error in {batch_name}: {str(e)}")
            await asyncio.sleep(1)
            continue
    return []

async def get_balances(session, addresses: List[str]) -> Dict[str, float]:
  
    balances = {}
    uncached_addresses = [addr for addr in addresses if addr not in cache['balances']]
    
    if uncached_addresses:
        print(f"Fetching balances for {len(uncached_addresses)} addresses...")
        for addr in uncached_addresses:
            try:
                if addr in cache['balances']:
                    balances[addr] = cache['balances'][addr]
                    continue

                response = await retry_request(session, "getBalance", [[addr]], f"balance check for {addr}")
                if response and len(response) > 0 and 'result' in response[0]:
                    balance = response[0]['result']['value'] / 1_000_000_000
                    balances[addr] = balance
                    cache['balances'][addr] = balance
                    print(f"Balance for {addr}: {balance} SOL")
                else:
                    balances[addr] = None
                    cache['balances'][addr] = None
                    print(f"Failed to get balance for {addr}")
                await asyncio.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"Error processing balance for {addr}: {str(e)}")
                balances[addr] = None
                cache['balances'][addr] = None
    else:
        print("Using cached balances")
        balances = {addr: cache['balances'].get(addr) for addr in addresses}
    
    save_cache()
    return balances

async def get_recent_transactions(session, addresses: List[str]) -> Dict[str, List[str]]:

    transactions = {}
    uncached_addresses = [addr for addr in addresses if addr not in cache['transactions']]
    
    if uncached_addresses:
        print(f"Fetching transactions for {len(uncached_addresses)} addresses...")
        for addr in uncached_addresses:
            try:
                response = await retry_request(
                    session,
                    "getSignaturesForAddress",
                    [[addr, {"limit": TRANSACTION_LIMIT}]],
                    f"transaction check for {addr}"
                )
                if response and len(response) > 0 and 'result' in response[0]:
                    transactions[addr] = response[0]['result']
                    cache['transactions'][addr] = response[0]['result']
                    print(f"Found {len(transactions[addr])} transactions for {addr}")
                else:
                    transactions[addr] = []
                    cache['transactions'][addr] = []
                    print(f"No transactions found for {addr}")
                await asyncio.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"Error processing transactions for {addr}: {str(e)}")
                transactions[addr] = []
                cache['transactions'][addr] = []
    else:
        print("Using cached transactions")
        transactions = {addr: cache['transactions'].get(addr, []) for addr in addresses}
    
    save_cache()
    return transactions


async def process_transaction(session, tx_signature: str, valid_addresses: Set[str]):

    if tx_signature in cache['transaction_details']:
        return cache['transaction_details'][tx_signature]
        
    try:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        response = await retry_request(
            session,
            "getTransaction",
            [[tx_signature, {"encoding": "jsonParsed"}]],
            f"tx details for {tx_signature[:8]}"
        )
        
        if response and len(response) > 0 and 'result' in response[0]:
            tx_data = response[0]['result']
            if tx_data and 'transaction' in tx_data:
                accounts = tx_data['transaction']['message']['accountKeys']
                related_accounts = set()
                for account in accounts:
                    if account['pubkey'] in valid_addresses:
                        related_accounts.add(account['pubkey'])
                cache['transaction_details'][tx_signature] = related_accounts
                save_cache()
                return related_accounts
        return set()
    except Exception as e:
        print(f"Error processing transaction {tx_signature[:8]}: {str(e)}")
        return set()

async def process_address_batch(session, addresses: List[str]):

    print(f"\nProcessing batch of {len(addresses)} addresses...")
    

    balances = await get_balances(session, addresses)

    for addr, balance in balances.items():
        if balance is not None:
            df.loc[df[0] == addr, 'Balance'] = balance

    transactions = await get_recent_transactions(session, addresses)
    
    return transactions

async def main():
    print("Starting main processing...")
    start_time = time.time()
    
 
    valid_addresses = set()
    current_cluster = None
    
    for index, row in df.iterrows():
        address = row[0]
        if pd.isna(address):
            continue
        if str(address).startswith('Cluster'):
            current_cluster = address.strip(':')
            print(f"\nProcessing {current_cluster}")
            continue
        if current_cluster and not pd.isna(address):
            valid_addresses.add(address)

    print(f"\nFound {len(valid_addresses)} valid addresses")


    address_batches = [list(valid_addresses)[i:i + BATCH_SIZE] 
                      for i in range(0, len(valid_addresses), BATCH_SIZE)]
    
    print(f"Split into {len(address_batches)} batches")


    address_graph = defaultdict(set)
    interaction_count = defaultdict(int)

    async with aiohttp.ClientSession() as session:
        for batch_num, batch in enumerate(address_batches, 1):
            print(f"\nProcessing batch {batch_num}/{len(address_batches)}")
            transactions = await process_address_batch(session, batch)
            
          
            for addr, txs in transactions.items():
                for tx in txs:
                    if 'signature' in tx:
                        related_accounts = await process_transaction(
                            session, 
                            tx['signature'], 
                            valid_addresses
                        )
                        
                        if len(related_accounts) > 1:
                            for acc1, acc2 in itertools.combinations(related_accounts, 2):
                                address_graph[acc1].add(acc2)
                                address_graph[acc2].add(acc1)
                                interaction_count[(acc1, acc2)] += 1
                                interaction_count[(acc2, acc1)] += 1

    end_time = time.time()
    print(f"\nProcessing completed in {(end_time - start_time) / 60:.2f} minutes")
    return address_graph, interaction_count

try:

    print("Reading Excel file...")
    df = pd.read_excel('multicAIn capital DAOs.xlsx', header=None)
    print(f"Successfully loaded {len(df)} rows from Excel")


    df['Balance'] = ''
    df['Whitelist Recommendation'] = ''
    df['Related Addresses'] = ''
    df['Risk Score'] = ''


    print("\nStarting async processing...")
    loop = asyncio.get_event_loop()
    address_graph, interaction_count = loop.run_until_complete(main())


    print("\nAnalyzing address relationships...")
    address_groups = []
    processed_addresses = set()

    for address in df[0].dropna():
        if address in processed_addresses or str(address).startswith('Cluster'):
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


    print("\nUpdating recommendations and risk scores...")
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


    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'multicAIn_capital_DAOs_analysis_{timestamp}.xlsx'
    df.to_excel(output_file, index=False, header=True)
    print(f"\nResults saved to {output_file}")


    print("\nAnalysis Summary:")
    print(f"Total address groups found: {len(address_groups)}")
    for i, group in enumerate(address_groups, 1):
        print(f"\nGroup {i} (Size: {len(group)}):")
        for addr in group:
            balance = df.loc[df[0] == addr, 'Balance'].iloc[0]
            print(f"  Address: {addr}, Balance: {balance:.3f} SOL")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    import traceback
    print(traceback.format_exc())
finally:

    save_cache()

print("\nProgram completed!")