import pandas as pd
import requests
import time
import json
from tqdm import tqdm


SOL_THRESHOLD = 0.5 


df = pd.read_excel('multicAIn capital DAOs.xlsx', header=None)


df['Balance'] = ''
df['Whitelist Recommendation'] = ''

# Solana RPC endpoint
url = "https://api.mainnet-beta.solana.com"

def get_balance(address):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [address]
    }
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        result = response.json()
        if 'result' in result:
            return result['result']['value'] / 1_000_000_000
    return None


cluster_stats = {}
current_cluster = None


valid_rows = df[~df[0].isna() & ~df[0].astype(str).str.startswith('Cluster')].index


for index in tqdm(range(len(df)), desc="Processing addresses"):
    row_value = df.at[index, 0]
    

    if pd.isna(row_value):
        continue
    
  
    if str(row_value).startswith('Cluster'):
        current_cluster = row_value.strip(':')
        cluster_stats[current_cluster] = {
            'total_addresses': 0,
            'recommended_addresses': 0,
            'total_balance': 0,
            'addresses_above_threshold': []
        }
        continue
    

    try:
        balance = get_balance(row_value)
        if balance is not None:
            df.at[index, 'Balance'] = balance
            
 
            cluster_stats[current_cluster]['total_addresses'] += 1
            cluster_stats[current_cluster]['total_balance'] += balance
            

            if balance >= SOL_THRESHOLD:
                df.at[index, 'Whitelist Recommendation'] = 'Yes'
                cluster_stats[current_cluster]['recommended_addresses'] += 1
                cluster_stats[current_cluster]['addresses_above_threshold'].append(row_value)
            else:
                df.at[index, 'Whitelist Recommendation'] = 'No'
        else:
            df.at[index, 'Balance'] = 'Error'
            df.at[index, 'Whitelist Recommendation'] = 'Error'
        
        time.sleep(0.5)
        
    except Exception as e:
        print(f"Error getting balance for {row_value}: {str(e)}")
        df.at[index, 'Balance'] = 'Error'
        df.at[index, 'Whitelist Recommendation'] = 'Error'


print("\nCluster Analysis Summary:")
for cluster, stats in cluster_stats.items():
    print(f"\n{cluster}:")
    print(f"Total addresses: {stats['total_addresses']}")
    print(f"Recommended addresses: {stats['recommended_addresses']}")
    print(f"Average balance: {stats['total_balance'] / stats['total_addresses']:.3f} SOL")
    print(f"Recommendation rate: {(stats['recommended_addresses'] / stats['total_addresses'] * 100):.1f}%")
