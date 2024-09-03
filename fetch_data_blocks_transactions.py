import subprocess
import json
import pandas as pd

from concurrent.futures import ProcessPoolExecutor, as_completed

class NexaCLI:
    def __init__(self, cli_path, rpc_user, rpc_password):
        self.cli_path = f'"{cli_path}"'
        self.rpc_user = rpc_user
        self.rpc_password = rpc_password

    def run_command(self, command):
        full_command = f"{self.cli_path} -rpcuser={self.rpc_user} -rpcpassword={self.rpc_password} {command}"
        try:
            result = subprocess.run(full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            if result.stdout:
                return result.stdout.strip()
            elif result.stderr:
                print(f"Error: {result.stderr.strip()}")
                return ""
        except Exception as e:
            print(f"Exception occurred: {e}")
            return ""

def get_latest_block_height(cli):
    # Get the latest block height
    command = "getblockcount"
    output = cli.run_command(command)
    if output.isdigit():
        return int(output.strip())
    else:
        print(f"Failed to retrieve block count: {output}")
        return None

def get_block_data(cli, block_height):
    command = f"getblockhash {block_height}"
    block_hash = cli.run_command(command).strip()

    if not block_hash:
        print(f"Failed to retrieve block hash for height {block_height}")
        return {}

    command = f"getblock {block_hash} 1"
    block_data_output = cli.run_command(command)

    try:
        block_data = json.loads(block_data_output)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError for block {block_height}: {e}")
        print(f"Command output: {block_data_output}")
        return {}

    return {block_height: block_data}

def get_transaction_data(cli, txid):
    command = f"getrawtransaction {txid} 1"
    output = cli.run_command(command)

    if "error code: -5" in output:
        print(f"getrawtransaction failed for {txid}, transaction might not be indexed or is not in the mempool.")
        return None

    if not output:
        print(f"Error: No output for transaction {txid}")
        return None

    try:
        transaction_data = json.loads(output)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError for transaction {txid}: {e}")
        print(f"Command output: {output}")
        return None

    return transaction_data

def get_latest_n_blocks(cli, n, num_processes=None):
    latest_block_height = get_latest_block_height(cli)
    if latest_block_height is None:
        return {}

    if n > latest_block_height + 1:
        print(f"Warning: Requested {n} blocks, but only {latest_block_height + 1} are available. Adjusting to {latest_block_height + 1}.")
        n = latest_block_height + 1

    block_heights = [latest_block_height - i for i in range(n)]

    print(f"Starting block data retrieval with {num_processes} parallel processes...")

    blocks_dict = {}
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {executor.submit(get_block_data, cli, block_height): block_height for block_height in block_heights}
        for i, future in enumerate(as_completed(futures)):
            block_data = future.result()
            blocks_dict.update(block_data)

            # Print progress every 10 iterations
            if (i + 1) % 10 == 0 or (i + 1) == len(block_heights):
                percentage = ((i + 1) / len(block_heights)) * 100
                print(f"Retrieved block {list(block_data.keys())[0]} ({i + 1}/{len(block_heights)}) - {percentage:.2f}% complete")

    return blocks_dict



def get_all_transactions(cli, blocks_dict, num_processes=None):
    txids = [txid for block_data in blocks_dict.values() for txid in block_data.get('txid', [])]

    print(f"Starting transaction data retrieval with {num_processes} parallel processes...")

    transactions_data = []
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {executor.submit(get_transaction_data, cli, txid): txid for txid in txids}
        for i, future in enumerate(as_completed(futures)):
            tx_data = future.result()
            if tx_data:
                transactions_data.append(tx_data)
            # Print real-time progress
            percentage = ((i + 1) / len(txids)) * 100
            if i % 10 == 0:  # Print every 10 iterations
                print(f"Processed transaction ({i + 1}/{len(txids)}) - {percentage:.2f}% complete")

    return transactions_data

def save_block_data(df_blocks, save_json=False):
    if save_json:
        json_file_name = "nexa_last_blocks.json"
        df_blocks.to_json(json_file_name, orient='records')
        print(f"Block data saved to {json_file_name}")

def save_transaction_data(transactions_data, save_json=False):
    if save_json:
        json_file_name = "nexa_transactions.json"
        with open(json_file_name, "w") as f:
            json.dump(transactions_data, f, indent=4)
        print(f"Transaction data saved to {json_file_name}")
        
if __name__ == "__main__":
    cli_path = "C:/Program Files/Nexa/daemon/nexa-cli"
    rpc_user = "myusername"
    rpc_password = "mypassword"
    cli = NexaCLI(cli_path, rpc_user, rpc_password)

    n_blocks = 100
    num_processes = 8  # You can adjust this number based on your system's capabilities

    blocks_dict = get_latest_n_blocks(cli, n_blocks, num_processes)
    df_blocks = pd.DataFrame.from_dict(blocks_dict, orient='index')

    print(df_blocks)

    save_block_data(df_blocks, save_json=True)

    transactions_dict = get_all_transactions(cli, blocks_dict, num_processes)

    save_transaction_data(transactions_dict, save_json=True)
