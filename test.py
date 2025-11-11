"""
Module for fetching innovative drug concept stocks data from A-share and HK markets.
Support incremental updates.
"""

import datetime
import time
import random
import pandas as pd
import akshare as ak
from typing import Optional
import os


def _setup_pandas_options():
    """Configure pandas display options."""
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)
    pd.set_option('display.max_rows', 1000)


def _get_a_stock_data(symbol: str, name: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Fetch A-share stock historical data with retry mechanism.
    
    Args:
        symbol: Stock code
        name: Stock name
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with stock history or None if failed
    """
    max_retries = 2
    retry_delay = 0.1  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching A-share data: {symbol} - {name} (attempt {attempt + 1})")
            
            stock_data = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if stock_data.empty:
                print(f"Warning: {symbol} returned empty data")
                return None
                
            stock_data = stock_data[['日期', '股票代码', '开盘', '收盘', 
                                   '最高', '最低', '成交量', '成交额']]
            stock_data.rename(
                columns={
                    '日期': 'Date',
                    '股票代码': 'Code',
                    '开盘': 'Open',
                    '收盘': 'Close',
                    '最高': 'High',
                    '最低': 'Low',
                    '成交量': 'Volume',
                    '成交额': 'Trading_Volume'
                },
                inplace=True
            )
            stock_data['Name'] = name
            stock_data['Type'] = 'A'
            
            return stock_data
            
        except Exception as e:
            print(f"Error fetching A-share data {symbol} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to fetch {symbol} after {max_retries} attempts")
                return None


def _get_hk_stock_data(symbol: str, name: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Fetch HK stock historical data with retry mechanism.
    
    Args:
        symbol: Stock code
        name: Stock name
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        DataFrame with stock history or None if failed
    """
    max_retries = 3
    retry_delay = 0.1  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching HK data: {symbol} - {name} (attempt {attempt + 1})")
            
            stock_data = ak.stock_hk_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if stock_data.empty:
                print(f"Warning: {symbol} returned empty data")
                return None
                
            stock_data = stock_data[['日期', '开盘', '收盘', '最高', 
                                   '最低', '成交量', '成交额']]
            stock_data.rename(
                columns={
                    '日期': 'Date',
                    '开盘': 'Open',
                    '收盘': 'Close',
                    '最高': 'High',
                    '最低': 'Low',
                    '成交量': 'Volume',
                    '成交额': 'Trading_Volume'
                },
                inplace=True
            )
            stock_data['Name'] = name
            stock_data['Code'] = symbol
            stock_data['Type'] = 'HK'
            
            return stock_data
            
        except Exception as e:
            print(f"Error fetching HK data {symbol} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                sleep_time = retry_delay * (attempt + 1)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed to fetch {symbol} after {max_retries} attempts")
                return None


def _process_a_stock_list(a_stock_list: pd.DataFrame, 
                          all_stock_data: pd.DataFrame,
                          start_date: str,
                          end_date: str) -> tuple[pd.DataFrame, list]:
    """
    Process A-share stock list and return updated data and failed stocks.
    
    Args:
        a_stock_list: DataFrame with A-share stock list
        all_stock_data: Accumulated stock data
        start_date: Start date for data fetching
        end_date: End date for data fetching
        
    Returns:
        Tuple of (updated_data, failed_stocks_list)
    """
    failed_stocks = []
    
    for i, (_, row) in enumerate(a_stock_list.iterrows()):
        stock_code = str(row['代码']).zfill(6)
        stock_name = row['名称']
        stock_data = _get_a_stock_data(stock_code, stock_name, start_date, end_date)
        
        if stock_data is not None:
            all_stock_data = pd.concat([all_stock_data, stock_data], 
                                     ignore_index=True)
        else:
            failed_stocks.append({'代码': stock_code, '名称': stock_name})
            print(f"记录失败股票: {stock_code} - {stock_name}")
        
        # Add delay between A-share requests
        time.sleep(0.5)
        
        # Progress update every 10 stocks
        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{len(a_stock_list)} A-share stocks")
    
    return all_stock_data, failed_stocks


def _process_hk_stock_list(hk_stock_list: pd.DataFrame, 
                           all_stock_data: pd.DataFrame,
                           start_date: str,
                           end_date: str) -> tuple[pd.DataFrame, list]:
    """
    Process HK stock list and return updated data and failed stocks.
    
    Args:
        hk_stock_list: DataFrame with HK stock list
        all_stock_data: Accumulated stock data
        start_date: Start date for data fetching
        end_date: End date for data fetching
        
    Returns:
        Tuple of (updated_data, failed_stocks_list)
    """
    failed_stocks = []
    
    for i, (_, row) in enumerate(hk_stock_list.iterrows()):
        stock_code = str(row['代码']).zfill(5)
        stock_name = row['名称']
        stock_data = _get_hk_stock_data(stock_code, stock_name, start_date, end_date)
        
        if stock_data is not None:
            all_stock_data = pd.concat([all_stock_data, stock_data], 
                                     ignore_index=True)
        else:
            failed_stocks.append({'代码': stock_code, '名称': stock_name})
            print(f"记录失败股票: {stock_code} - {stock_name}")
        
        # Longer delay for HK requests (more sensitive)
        time.sleep(1.5)
        
        # Progress update every 5 stocks
        if (i + 1) % 5 == 0:
            print(f"Processed {i + 1}/{len(hk_stock_list)} HK stocks")
    
    return all_stock_data, failed_stocks


def _load_existing_data(file_path: str) -> pd.DataFrame:
    """
    Load existing data from file if it exists.
    
    Args:
        file_path: Path to the existing data file
        
    Returns:
        DataFrame with existing data or empty DataFrame if file doesn't exist
    """
    if os.path.exists(file_path):
        print(f"Loading existing data from {file_path}")
        try:
            existing_data = pd.read_csv(file_path)
            # Ensure Date column is string type for consistent comparison
            existing_data['Date'] = pd.to_datetime(existing_data['Date']).dt.strftime('%Y-%m-%d')
            print(f"Loaded {len(existing_data)} existing records")
            
            # Display date range info
            if not existing_data.empty:
                date_min = existing_data['Date'].min()
                date_max = existing_data['Date'].max()
                print(f"Date range in existing data: {date_min} to {date_max}")
            
            return existing_data
        except Exception as e:
            print(f"Error loading existing data: {e}")
            return pd.DataFrame()
    else:
        print("No existing data file found, starting fresh")
        return pd.DataFrame()


def _get_incremental_dates(existing_data: pd.DataFrame, days_back: int = 365) -> tuple[str, str]:
    """
    Calculate start and end dates for incremental update.
    
    Args:
        existing_data: DataFrame with existing data
        days_back: Number of days to look back if no existing data
        
    Returns:
        Tuple of (start_date, end_date) in YYYYMMDD format
    """
    today = datetime.date.today()
    end_date = today.strftime('%Y%m%d')
    
    if existing_data.empty:
        # No existing data, fetch full period
        start_date = (today - datetime.timedelta(days=days_back)).strftime('%Y%m%d')
        print(f"No existing data, fetching full period: {start_date} to {end_date}")
    else:
        # Find the latest date in existing data
        latest_date_str = existing_data['Date'].max()
        # Convert to datetime object for calculation
        latest_date = pd.to_datetime(latest_date_str)
        # Use the next day as start date for incremental update
        start_date = (latest_date + datetime.timedelta(days=1)).strftime('%Y%m%d')
        print(f"Existing data latest date: {latest_date_str}")
        print(f"Fetching incremental data from: {start_date} to {end_date}")
        
        # If start_date is after end_date, no new data needed
        if start_date > end_date:
            print("No new data to fetch (start date after end date)")
            start_date = end_date
    
    return start_date, end_date


def _remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records based on Date, Code, and Type.
    
    Args:
        data: DataFrame with potential duplicates
        
    Returns:
        DataFrame with duplicates removed
    """
    initial_count = len(data)
    data = data.drop_duplicates(subset=['Date', 'Code', 'Type'], keep='last')
    final_count = len(data)
    
    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} duplicate records")
    
    # Ensure consistent date format
    data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
    
    return data.sort_values(['Code', 'Date']).reset_index(drop=True)


def _format_date_range_for_display(data: pd.DataFrame) -> str:
    """
    Format date range for display, handling string dates.
    
    Args:
        data: DataFrame with Date column
        
    Returns:
        Formatted date range string
    """
    if data.empty:
        return "No data"
    
    try:
        # Convert to datetime for proper comparison
        dates = pd.to_datetime(data['Date'])
        date_min = dates.min().strftime('%Y-%m-%d')
        date_max = dates.max().strftime('%Y-%m-%d')
        return f"{date_min} to {date_max}"
    except Exception as e:
        # Fallback to string comparison
        date_min = data['Date'].min()
        date_max = data['Date'].max()
        return f"{date_min} to {date_max}"


def get_innovative_drug_stocks_data(a_stock_csv_path: str, 
                                  hk_stock_csv_path: str,
                                  existing_data_path: str = 'innovative_drug_stocks_data.csv',
                                  max_retry_rounds: int = 5,
                                  retry_delay_minutes: int = 5) -> pd.DataFrame:
    """
    Fetch innovative drug concept stocks data from A-share and HK markets.
    Automatically retries failed stocks after waiting period.
    Supports incremental updates.
    
    Args:
        a_stock_csv_path: Path to A-share stock list CSV
        hk_stock_csv_path: Path to HK stock list CSV
        existing_data_path: Path to existing data file for incremental update
        max_retry_rounds: Maximum number of retry rounds (default: 5)
        retry_delay_minutes: Minutes to wait before retry (default: 30)
        
    Returns:
        Combined DataFrame with A-share and HK data
        
    Raises:
        FileNotFoundError: When CSV files not found
        ValueError: When CSV format is invalid
    """
    _setup_pandas_options()
    
    # Load existing data
    existing_data = _load_existing_data(existing_data_path)
    
    # Calculate dates for incremental update
    start_date, end_date = _get_incremental_dates(existing_data)
    
    # Initialize with existing data
    all_stock_data = existing_data.copy()
    
    # Check if we need to fetch new data
    if start_date > end_date:
        print("No new data to fetch. Returning existing data.")
        return _remove_duplicates(all_stock_data)
    
    # Load initial stock lists
    try:
        print("Loading stock lists...")
        a_stock_list = pd.read_csv(a_stock_csv_path, encoding='utf-16', sep='\t')
        print(f"A-share list contains {len(a_stock_list)} stocks")
    except FileNotFoundError:
        print(f"Error: A-share CSV file not found {a_stock_csv_path}")
        raise
    except Exception as e:
        print(f"Error loading A-share CSV: {e}")
        raise ValueError(f"A-share CSV loading failed: {e}")
    
    try:
        hk_stock_list = pd.read_csv(hk_stock_csv_path, encoding='utf-16', sep='\t')
        print(f"HK list contains {len(hk_stock_list)} stocks")
    except FileNotFoundError:
        print(f"Error: HK CSV file not found {hk_stock_csv_path}")
        raise
    except Exception as e:
        print(f"Error loading HK CSV: {e}")
        print("Continuing with A-share data only...")
        hk_stock_list = pd.DataFrame(columns=['代码', '名称'])
    
    # Track failed stocks for retry
    failed_a_stocks = []
    failed_hk_stocks = []
    
    # Initial processing round
    round_num = 1
    print(f"\n{'='*60}")
    print(f"开始第 {round_num} 轮数据获取")
    print(f"数据期间: {start_date} 至 {end_date}")
    print(f"{'='*60}\n")
    
    # Process A-share data
    if len(a_stock_list) > 0:
        print("Processing A-share data...")
        all_stock_data, failed_a_stocks = _process_a_stock_list(
            a_stock_list, all_stock_data, start_date, end_date
        )
        print(f"A股处理完成，成功: {len(a_stock_list) - len(failed_a_stocks)}, "
              f"失败: {len(failed_a_stocks)}")
    
    # Process HK data
    if len(hk_stock_list) > 0:
        print("\nProcessing HK data...")
        all_stock_data, failed_hk_stocks = _process_hk_stock_list(
            hk_stock_list, all_stock_data, start_date, end_date
        )
        print(f"港股处理完成，成功: {len(hk_stock_list) - len(failed_hk_stocks)}, "
              f"失败: {len(failed_hk_stocks)}")
    
    # Retry failed stocks
    while (len(failed_a_stocks) > 0 or len(failed_hk_stocks) > 0) and round_num < max_retry_rounds:
        round_num += 1
        print(f"\n{'='*60}")
        print(f"第 {round_num} 轮重试开始")
        print(f"待重试 A股: {len(failed_a_stocks)} 只")
        print(f"待重试 港股: {len(failed_hk_stocks)} 只")
        print(f"{'='*60}\n")
        
        # Wait before retry
        wait_seconds = retry_delay_minutes * 60
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=wait_seconds)
        print(f"等待 {retry_delay_minutes} 分钟后开始重试...")
        print(f"开始等待时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"预计开始时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Countdown display
        elapsed = 0
        while elapsed < wait_seconds:
            remaining = wait_seconds - elapsed
            mins = remaining // 60
            secs = remaining % 60
            
            # Update display every 10 seconds, or every second in the last minute
            if remaining <= 60 or elapsed % 10 == 0:
                print(f"剩余等待时间: {mins:02d}分{secs:02d}秒", end='\r', flush=True)
            
            time.sleep(1)
            elapsed += 1
        print("\n等待完成，开始重试...\n")
        
        # Retry failed A-share stocks
        new_failed_a_stocks = []
        if len(failed_a_stocks) > 0:
            print(f"重试 A股数据 ({len(failed_a_stocks)} 只)...")
            failed_a_df = pd.DataFrame(failed_a_stocks)
            temp_data, new_failed_a_stocks = _process_a_stock_list(
                failed_a_df, all_stock_data, start_date, end_date
            )
            all_stock_data = temp_data
            print(f"A股重试完成，成功: {len(failed_a_stocks) - len(new_failed_a_stocks)}, "
                  f"仍失败: {len(new_failed_a_stocks)}")
        
        # Retry failed HK stocks
        new_failed_hk_stocks = []
        if len(failed_hk_stocks) > 0:
            print(f"\n重试 港股数据 ({len(failed_hk_stocks)} 只)...")
            failed_hk_df = pd.DataFrame(failed_hk_stocks)
            temp_data, new_failed_hk_stocks = _process_hk_stock_list(
                failed_hk_df, all_stock_data, start_date, end_date
            )
            all_stock_data = temp_data
            print(f"港股重试完成，成功: {len(failed_hk_stocks) - len(new_failed_hk_stocks)}, "
                  f"仍失败: {len(new_failed_hk_stocks)}")
        
        # Update failed lists
        failed_a_stocks = new_failed_a_stocks
        failed_hk_stocks = new_failed_hk_stocks
    
    # Remove duplicates and sort
    print("\nRemoving duplicates and sorting data...")
    all_stock_data = _remove_duplicates(all_stock_data)
    
    # Final summary
    print(f"\n{'='*60}")
    print("数据获取完成")
    print(f"{'='*60}")
    print(f"总记录数: {len(all_stock_data)}")
    if len(all_stock_data) > 0:
        print(f"成功获取股票数: {all_stock_data['Code'].nunique()}")
        date_range = _format_date_range_for_display(all_stock_data)
        print(f"数据期间: {date_range}")
    print(f"总轮数: {round_num}")
    
    # Calculate incremental stats
    if not existing_data.empty:
        new_records = len(all_stock_data) - len(existing_data)
        print(f"新增记录数: {new_records}")
    
    if len(failed_a_stocks) > 0 or len(failed_hk_stocks) > 0:
        print(f"\n警告: 仍有部分股票未能成功获取:")
        if len(failed_a_stocks) > 0:
            print(f"  A股失败: {len(failed_a_stocks)} 只")
            for stock in failed_a_stocks:
                print(f"    - {stock['代码']}: {stock['名称']}")
        if len(failed_hk_stocks) > 0:
            print(f"  港股失败: {len(failed_hk_stocks)} 只")
            for stock in failed_hk_stocks:
                print(f"    - {stock['代码']}: {stock['名称']}")

    all_stock_data = all_stock_data.drop_duplicates(subset=['Type', 'Code', 'Date'], keep='last')
    all_stock_data.sort_values(by=['Type', 'Code', 'Date'], inplace=True)
    all_stock_data.reset_index(drop=True, inplace=True)
    return all_stock_data


def main():
    """Main function."""
    a_stock_csv = ('Ashare_innovative_drug_stocks_code.csv')
    hk_stock_csv = ('Hshare_innovative_drug_stocks_code.csv')
    output_file = 'innovative_drug_stocks_data.csv'

    try:
        stock_data = get_innovative_drug_stocks_data(a_stock_csv, hk_stock_csv, output_file)
        
        print("\nData overview:")
        print(stock_data.head())
        print(f"\nData shape: {stock_data.shape}")
        print("Data type distribution:")
        print(stock_data['Type'].value_counts())
        
        # Save to the same file (incremental update)
        stock_data.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nData saved to: {output_file}")
        
    except Exception as e:
        print(f"Execution error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
