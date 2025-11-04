"""
Module for fetching innovative drug concept stocks data from A-share and HK markets.
"""

import datetime
import time
import random
import pandas as pd
import akshare as ak
from typing import Optional


def _setup_pandas_options():
    """Configure pandas display options."""
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)
    pd.set_option('display.max_rows', 1000)


def _get_a_stock_data(symbol: str, name: str) -> Optional[pd.DataFrame]:
    """
    Fetch A-share stock historical data with retry mechanism.
    
    Args:
        symbol: Stock code
        name: Stock name
        
    Returns:
        DataFrame with stock history or None if failed
    """
    max_retries = 2
    retry_delay = 0.1  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching A-share data: {symbol} - {name} (attempt {attempt + 1})")
            today = datetime.date.today().strftime('%Y%m%d')
            one_year_ago = (datetime.date.today() - 
                           datetime.timedelta(days=365)).strftime('%Y%m%d')
            
            stock_data = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=one_year_ago,
                end_date=today,
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


def _get_hk_stock_data(symbol: str, name: str) -> Optional[pd.DataFrame]:
    """
    Fetch HK stock historical data with retry mechanism.
    
    Args:
        symbol: Stock code
        name: Stock name
        
    Returns:
        DataFrame with stock history or None if failed
    """
    max_retries = 3
    retry_delay = 0.1  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching HK data: {symbol} - {name} (attempt {attempt + 1})")
            today = datetime.date.today().strftime('%Y%m%d')
            one_year_ago = (datetime.date.today() - 
                           datetime.timedelta(days=365)).strftime('%Y%m%d')
            
            stock_data = ak.stock_hk_hist(
                symbol=symbol,
                period="daily",
                start_date=one_year_ago,
                end_date=today,
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
                          all_stock_data: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Process A-share stock list and return updated data and failed stocks.
    
    Args:
        a_stock_list: DataFrame with A-share stock list
        all_stock_data: Accumulated stock data
        
    Returns:
        Tuple of (updated_data, failed_stocks_list)
    """
    failed_stocks = []
    
    for i, (_, row) in enumerate(a_stock_list.iterrows()):
        stock_code = str(row['代码']).zfill(6)
        stock_name = row['名称']
        stock_data = _get_a_stock_data(stock_code, stock_name)
        
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
                           all_stock_data: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Process HK stock list and return updated data and failed stocks.
    
    Args:
        hk_stock_list: DataFrame with HK stock list
        all_stock_data: Accumulated stock data
        
    Returns:
        Tuple of (updated_data, failed_stocks_list)
    """
    failed_stocks = []
    
    for i, (_, row) in enumerate(hk_stock_list.iterrows()):
        stock_code = str(row['代码']).zfill(5)
        stock_name = row['名称']
        stock_data = _get_hk_stock_data(stock_code, stock_name)
        
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


def get_innovative_drug_stocks_data(a_stock_csv_path: str, 
                                  hk_stock_csv_path: str,
                                  max_retry_rounds: int = 5,
                                  retry_delay_minutes: int = 5) -> pd.DataFrame:
    """
    Fetch innovative drug concept stocks data from A-share and HK markets.
    Automatically retries failed stocks after waiting period.
    
    Args:
        a_stock_csv_path: Path to A-share stock list CSV
        hk_stock_csv_path: Path to HK stock list CSV
        max_retry_rounds: Maximum number of retry rounds (default: 5)
        retry_delay_minutes: Minutes to wait before retry (default: 30)
        
    Returns:
        Combined DataFrame with A-share and HK data
        
    Raises:
        FileNotFoundError: When CSV files not found
        ValueError: When CSV format is invalid
    """
    _setup_pandas_options()
    
    all_stock_data = pd.DataFrame()
    
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
    print(f"{'='*60}\n")
    
    # Process A-share data
    if len(a_stock_list) > 0:
        print("Processing A-share data...")
        all_stock_data, failed_a_stocks = _process_a_stock_list(
            a_stock_list, all_stock_data
        )
        print(f"A股处理完成，成功: {len(a_stock_list) - len(failed_a_stocks)}, "
              f"失败: {len(failed_a_stocks)}")
    
    # Process HK data
    if len(hk_stock_list) > 0:
        print("\nProcessing HK data...")
        all_stock_data, failed_hk_stocks = _process_hk_stock_list(
            hk_stock_list, all_stock_data
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
        last_minute = wait_seconds // 60
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
                failed_a_df, all_stock_data
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
                failed_hk_df, all_stock_data
            )
            all_stock_data = temp_data
            print(f"港股重试完成，成功: {len(failed_hk_stocks) - len(new_failed_hk_stocks)}, "
                  f"仍失败: {len(new_failed_hk_stocks)}")
        
        # Update failed lists
        failed_a_stocks = new_failed_a_stocks
        failed_hk_stocks = new_failed_hk_stocks
    
    # Final summary
    print(f"\n{'='*60}")
    print("数据获取完成")
    print(f"{'='*60}")
    print(f"总记录数: {len(all_stock_data)}")
    if len(all_stock_data) > 0:
        print(f"成功获取股票数: {all_stock_data['Code'].nunique()}")
    print(f"总轮数: {round_num}")
    
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
    
    return all_stock_data


def main():
    """Main function."""
    a_stock_csv = ('Ashare_innovative_drug_stocks_code.csv')
    hk_stock_csv = ('Hshare_innovative_drug_stocks_code.csv')

    try:
        stock_data = get_innovative_drug_stocks_data(a_stock_csv, hk_stock_csv)
        
        print("\nData overview:")
        print(stock_data.head())
        print(f"\nData shape: {stock_data.shape}")
        print("Data type distribution:")
        print(stock_data['Type'].value_counts())
        
        output_file = 'innovative_drug_stocks_data.csv'
        stock_data.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nData saved to: {output_file}")
        
    except Exception as e:
        print(f"Execution error: {e}")


if __name__ == "__main__":
    main()
