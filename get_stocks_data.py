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
    retry_delay = 1  # seconds
    
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
    retry_delay = 2  # seconds
    
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


def get_innovative_drug_stocks_data(a_stock_csv_path: str, 
                                  hk_stock_csv_path: str) -> pd.DataFrame:
    """
    Fetch innovative drug concept stocks data from A-share and HK markets.
    
    Args:
        a_stock_csv_path: Path to A-share stock list CSV
        hk_stock_csv_path: Path to HK stock list CSV
        
    Returns:
        Combined DataFrame with A-share and HK data
        
    Raises:
        FileNotFoundError: When CSV files not found
        ValueError: When CSV format is invalid
    """
    _setup_pandas_options()
    
    all_stock_data = pd.DataFrame()
    
    # Process A-share data
    try:
        print("Processing A-share data...")
        a_stock_list = pd.read_csv(a_stock_csv_path, encoding='utf-16', sep='\t')
        print(f"A-share list contains {len(a_stock_list)} stocks")
        
        for i, (_, row) in enumerate(a_stock_list.iterrows()):
            stock_code = str(row['代码']).zfill(6)
            stock_data = _get_a_stock_data(stock_code, row['名称'])
            if stock_data is not None:
                all_stock_data = pd.concat([all_stock_data, stock_data], 
                                         ignore_index=True)
            
            # Add delay between A-share requests
            time.sleep(0.5)
            
            # Progress update every 10 stocks
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(a_stock_list)} A-share stocks")
                
    except FileNotFoundError:
        print(f"Error: A-share CSV file not found {a_stock_csv_path}")
        raise
    except Exception as e:
        print(f"Error processing A-share data: {e}")
        raise ValueError(f"A-share data processing failed: {e}")
    
    # Process HK data with longer delays
    try:
        print("Processing HK data...")
        hk_stock_list = pd.read_csv(hk_stock_csv_path, encoding='utf-16', sep='\t')
        print(f"HK list contains {len(hk_stock_list)} stocks")
        
        for i, (_, row) in enumerate(hk_stock_list.iterrows()):
            stock_code = str(row['代码']).zfill(5)
            stock_data = _get_hk_stock_data(stock_code, row['名称'])
            if stock_data is not None:
                all_stock_data = pd.concat([all_stock_data, stock_data], 
                                         ignore_index=True)
            
            # Longer delay for HK requests (more sensitive)
            time.sleep(1.5)
            
            # Progress update every 5 stocks
            if (i + 1) % 5 == 0:
                print(f"Processed {i + 1}/{len(hk_stock_list)} HK stocks")
                
    except FileNotFoundError:
        print(f"Error: HK CSV file not found {hk_stock_csv_path}")
        raise
    except Exception as e:
        print(f"Error processing HK data: {e}")
        # Don't raise for HK data, continue with available data
        print("Continuing with available data...")
    
    print(f"Data fetch complete. Total records: {len(all_stock_data)}")
    print(f"Successfully fetched {all_stock_data['Code'].nunique()} unique stocks")
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
