import json
import pandas as pd
import matplotlib.pyplot as plt

def read_json_file(file_path):
    """
    Reads a JSON file and returns its content.
    
    :param file_path: Path to the JSON file.
    :return: Data contained in the JSON file.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        print(f"Successfully read JSON file: {file_path}")
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def process_block_data(block_data):
    """
    Processes block data into a DataFrame with a datetime index.
    
    :param block_data: List of block dictionaries.
    :return: DataFrame with processed block data.
    """
    # Convert block data to DataFrame
    df_blocks = pd.DataFrame(block_data)
    
    # Filter the desired columns from df_blocks and make a copy to avoid SettingWithCopyWarning
    columns_to_keep_blocks = ['height', 'size', 'txcount', 'time', 'mediantime', 'difficulty']
    df_filtered_blocks = df_blocks[columns_to_keep_blocks].copy()

    # Convert 'time' column to datetime and set as index for df_filtered_blocks
    df_filtered_blocks.loc[:, 'time'] = pd.to_datetime(df_filtered_blocks['time'], unit='s')
    df_filtered_blocks.set_index('time', inplace=True)

    # Sort df_filtered_blocks in ascending order by the datetime index
    df_filtered_blocks.sort_index(ascending=True, inplace=True)

    return df_filtered_blocks

def process_transaction_data(transaction_data):
    """
    Processes transaction data into a DataFrame with a datetime index.
    
    :param transaction_data: List of transaction dictionaries.
    :return: DataFrame with processed transaction data.
    """
    # Convert the list of transaction dictionaries into a DataFrame
    df_transactions = pd.DataFrame(transaction_data)

    # Create a new DataFrame with only the desired columns for transactions and make a copy to avoid SettingWithCopyWarning
    columns_to_keep_transactions = ['size', 'locktime', 'spends', 'sends', 'fee', 'blockindex', 'blocktime', 'time', 'confirmations']
    df_filtered_transactions = df_transactions[columns_to_keep_transactions].copy()

    # Convert 'time' column to datetime and set as index for df_filtered_transactions
    df_filtered_transactions.loc[:, 'time'] = pd.to_datetime(df_filtered_transactions['time'], unit='s')
    df_filtered_transactions.set_index('time', inplace=True)

    # Sort df_filtered_transactions in ascending order by the datetime index
    df_filtered_transactions.sort_index(ascending=True, inplace=True)

    return df_filtered_transactions

def calculate_hourly_volume_and_fees(df_filtered_transactions):
    """
    Calculates hourly volume and fees aggregation based on the 'sends' and 'fee' columns.
    
    :param df_filtered_transactions: DataFrame with transaction data and datetime index.
    :return: DataFrame with hourly aggregated volume and fees.
    """
    # Ensure 'sends' and 'fee' columns are numeric
    df_filtered_transactions['sends'] = pd.to_numeric(df_filtered_transactions['sends'], errors='coerce')
    df_filtered_transactions['fee'] = pd.to_numeric(df_filtered_transactions['fee'], errors='coerce')

    # Resample the data to hourly frequency and calculate the sum of 'sends' and 'fee' for each hour
    df_hourly_aggregation = df_filtered_transactions.resample('H').agg({'sends': 'sum', 'fee': 'sum'})

    # Rename columns for clarity
    df_hourly_aggregation.rename(columns={'sends': 'hourly_volume', 'fee': 'hourly_fees'}, inplace=True)

    return df_hourly_aggregation

def calculate_transactions_per_hour(df_filtered_transactions):
    """
    Calculates the number of transactions per hour.
    
    :param df_filtered_transactions: DataFrame with transaction data and datetime index.
    :return: DataFrame with the count of transactions per hour.
    """
    # Resample the data to hourly frequency and count the number of transactions for each hour
    df_transactions_per_hour = df_filtered_transactions.resample('H').size()

    # Convert the series to a DataFrame for easier handling
    df_transactions_per_hour = df_transactions_per_hour.to_frame(name='transactions_per_hour')

    return df_transactions_per_hour

def calculate_hourly_closing_difficulty(df_filtered_blocks):
    """
    Calculates the closing difficulty level for each hour.
    
    :param df_filtered_blocks: DataFrame with block data and datetime index.
    :return: DataFrame with the closing difficulty level for each hour.
    """
    # Resample the data to hourly frequency and get the last difficulty value for each hour
    df_hourly_closing_difficulty = df_filtered_blocks['difficulty'].resample('H').last()

    # Convert the series to a DataFrame for easier handling
    df_hourly_closing_difficulty = df_hourly_closing_difficulty.to_frame(name='closing_difficulty')

    return df_hourly_closing_difficulty

def plot_time_series(df_hourly_aggregation, df_transactions_per_hour, df_hourly_closing_difficulty):
    """
    Plots the time series data including hourly volume, fees, number of transactions, and difficulty.
    
    :param df_hourly_aggregation: DataFrame with hourly aggregated volume and fees.
    :param df_transactions_per_hour: DataFrame with number of transactions per hour.
    :param df_hourly_closing_difficulty: DataFrame with closing difficulty level for each hour.
    """
    plt.figure(figsize=(15, 10))

    # Plot hourly volume and fees
    plt.subplot(3, 1, 1)
    plt.plot(df_hourly_aggregation.index, df_hourly_aggregation['hourly_volume'], label='Hourly Volume', color='blue')
    plt.plot(df_hourly_aggregation.index, df_hourly_aggregation['hourly_fees'], label='Hourly Fees', color='green')
    plt.title('Hourly Transaction Volume and Fees')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True)

    # Plot number of transactions per hour
    plt.subplot(3, 1, 2)
    plt.bar(df_transactions_per_hour.index, df_transactions_per_hour['transactions_per_hour'], width=0.03, color='orange', label='Transactions per Hour')
    plt.title('Number of Transactions per Hour')
    plt.xlabel('Time')
    plt.ylabel('Number of Transactions')
    plt.legend()
    plt.grid(True)

    # Plot hourly closing difficulty
    plt.subplot(3, 1, 3)
    plt.plot(df_hourly_closing_difficulty.index, df_hourly_closing_difficulty['closing_difficulty'], label='Closing Difficulty', color='red')
    plt.title('Hourly Closing Difficulty')
    plt.xlabel('Time')
    plt.ylabel('Difficulty')
    plt.legend()
    plt.grid(True)

    # Display the plots
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # File paths to the JSON files
    block_data_file_path = "nexa_last_blocks.json"  # Replace with your actual file path if different
    transaction_data_file_path = "nexa_transactions.json"  # Example filename; update with your actual timestamped file name

    # Read and process block data
    block_data = read_json_file(block_data_file_path)
    if block_data:
        df_filtered_blocks = process_block_data(block_data)
        print("\nFiltered Block DataFrame with Datetime Index (Sorted in Ascending Order):")
        print(df_filtered_blocks)

        # Calculate hourly closing difficulty
        df_hourly_closing_difficulty = calculate_hourly_closing_difficulty(df_filtered_blocks)
        print("\nHourly Closing Difficulty DataFrame:")
        print(df_hourly_closing_difficulty)

    # Read and process transaction data
    transaction_data = read_json_file(transaction_data_file_path)
    if transaction_data:
        df_filtered_transactions = process_transaction_data(transaction_data)
        print("\nFiltered Transaction DataFrame with Datetime Index (Sorted in Ascending Order):")
        print(df_filtered_transactions)

        # Calculate hourly volume and fees aggregation
        df_hourly_aggregation = calculate_hourly_volume_and_fees(df_filtered_transactions)
        print("\nHourly Volume and Fees Aggregation DataFrame:")
        print(df_hourly_aggregation)

        # Calculate the number of transactions per hour
        df_transactions_per_hour = calculate_transactions_per_hour(df_filtered_transactions)
        print("\nNumber of Transactions per Hour DataFrame:")
        print(df_transactions_per_hour)

        # Plot the time series data
        plot_time_series(df_hourly_aggregation, df_transactions_per_hour, df_hourly_closing_difficulty)
