"""
Module for fetching innovative drug concept stocks data from A-share.
"""

import datetime
import time
import random
import json
import numpy as np
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



def _write_log_file(completion_time: datetime.datetime, 
                   failed_a_stocks: list, 
                   total_stocks: int,
                   successful_stocks: int) -> None:
    """
    Write log file with program completion time and failed stocks information.
    
    Args:
        completion_time: When the program completed
        failed_a_stocks: List of failed A-share stocks
        total_stocks: Total number of stocks to fetch
        successful_stocks: Number of successfully fetched stocks
    """
    log_filename = "stock_data_fetch_log.txt"
    
    with open(log_filename, 'a', encoding='utf-8') as log_file:
        log_file.write("\n" + "=" * 60 + "\n")
        log_file.write("股票数据获取程序执行日志\n")
        log_file.write("=" * 60 + "\n")
        
        # 1. 程序完成时间
        log_file.write(f"程序执行完成时间: {completion_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 2. 股票数据获取结果
        log_file.write("股票数据获取结果:\n")
        log_file.write(f"  总股票数量: {total_stocks}\n")
        log_file.write(f"  成功获取: {successful_stocks}\n")
        log_file.write(f"  失败数量: {len(failed_a_stocks) }\n")
        log_file.write(f"  成功率: {successful_stocks/total_stocks*100:.2f}%\n\n")
        
        # 失败的股票详情
        if failed_a_stocks :
            log_file.write("失败的股票列表:\n")
            
            if failed_a_stocks:
                log_file.write("  A股失败股票:\n")
                for i, stock in enumerate(failed_a_stocks, 1):
                    log_file.write(f"    {i}. {stock['代码']} - {stock['名称']}\n")
                log_file.write("\n")

        else:
            log_file.write("所有股票数据均已成功获取！\n")
        
        log_file.write("=" * 60 + "\n")
    
    print(f"\n日志已追加到文件: {log_filename}")


def get_innovative_drug_stocks_data(a_stock_json_path: str, 
                                  max_retry_rounds: int = 5,
                                  retry_delay_minutes: int = 45) -> pd.DataFrame:
    """
    Fetch innovative drug concept stocks data from A-share.
    Automatically retries failed stocks after waiting period.
    
    Args:
        a_stock_json_path: Path to A-share stock list JSON
        max_retry_rounds: Maximum number of retry rounds (default: 5)
        retry_delay_minutes: Minutes to wait before retry (default: 30)
        
    Returns:
        Combined DataFrame with A-share
        
    Raises:
        FileNotFoundError: When JSON files not found
        ValueError: When JSON format is invalid
    """
    _setup_pandas_options()
    
    all_stock_data = pd.DataFrame()
    
    # Load initial stock lists
    try:
        print("Loading stock lists...")
        with open(a_stock_json_path, 'r', encoding='utf-8') as f:
            a_stock_data = json.load(f)
        a_stock_list = pd.DataFrame(a_stock_data)
        print(f"A-share list contains {len(a_stock_list)} stocks")
    except FileNotFoundError:
        print(f"Error: A-share JSON file not found {a_stock_json_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing A-share JSON: {e}")
        raise ValueError(f"A-share JSON parsing failed: {e}")
    except Exception as e:
        print(f"Error loading A-share JSON: {e}")
        raise ValueError(f"A-share JSON loading failed: {e}")
    
    # Track failed stocks for retry
    failed_a_stocks = []
    
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

    
    # Retry failed stocks
    while (len(failed_a_stocks) > 0 ) and round_num < max_retry_rounds:
        round_num += 1
        print(f"\n{'='*60}")
        print(f"第 {round_num} 轮重试开始")
        print(f"待重试 A股: {len(failed_a_stocks)} 只")
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
        
        # Update failed lists
        failed_a_stocks = new_failed_a_stocks
    
    # Final summary
    print(f"\n{'='*60}")
    print("数据获取完成")
    print(f"{'='*60}")
    print(f"总记录数: {len(all_stock_data)}")
    successful_stocks = 0
    if len(all_stock_data) > 0:
        successful_stocks = all_stock_data['Code'].nunique()
        print(f"成功获取股票数: {successful_stocks}")
    print(f"总轮数: {round_num}")
    
    # Calculate total stocks count
    total_stocks = len(a_stock_list)
    
    # Write log file
    completion_time = datetime.datetime.now()
    _write_log_file(completion_time, failed_a_stocks, 
                   total_stocks, successful_stocks)
    
    if len(failed_a_stocks) > 0 :
        print(f"\n警告: 仍有部分股票未能成功获取:")
        if len(failed_a_stocks) > 0:
            print(f"  A股失败: {len(failed_a_stocks)} 只")
            for stock in failed_a_stocks:
                print(f"    - {stock['代码']}: {stock['名称']}")
    
    return all_stock_data


def main():
    """Main function."""
    a_stock_json = ('Ashare_innovative_drug_stocks_code.json')

    try:
        stock_data = get_innovative_drug_stocks_data(a_stock_json)
        
        print("\nData overview:")
        print(stock_data.head())
        print(f"\nData shape: {stock_data.shape}")
        print("Data type distribution:")
        print(stock_data['Type'].value_counts())
        
        output_file = 'innovative_drug_stocks_data.json'
        # Convert DataFrame to JSON format
        # Convert date columns to string format for JSON serialization
        stock_data_copy = stock_data.copy()
        # Convert Date column to string if it exists
        if 'Date' in stock_data_copy.columns:
            stock_data_copy['Date'] = stock_data_copy['Date'].astype(str)
        # Convert DataFrame to dict, handling NaN values and numpy types
        stock_data_json = stock_data_copy.to_dict(orient='records')
        # Replace NaN/NaT values and convert numpy types for JSON serialization
        for record in stock_data_json:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, (pd.Timestamp, datetime.date)):
                    record[key] = str(value)
                elif isinstance(value, (np.integer, np.floating)):
                    record[key] = value.item()
                elif isinstance(value, np.ndarray):
                    record[key] = value.tolist()
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(stock_data_json, f, ensure_ascii=False, indent=2)
        print(f"\nData saved to: {output_file}")
        
    except Exception as e:
        print(f"Execution error: {e}")


if __name__ == "__main__":
    main()
