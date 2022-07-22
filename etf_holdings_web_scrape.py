import pandas as pd
import os
from os.path import join
from pathlib import Path
import logging
import csv
import io
import requests
from datetime import datetime
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
logging.basicConfig(level=logging.INFO)

def get_tinfo():
    tinfo = pd.read_csv(join(Path.home(), '.zipline\custom_data\SHARADAR_TICKERS.csv'))
    tinfo = tinfo[['permaticker', 'ticker', 'name', 'exchange', 'isdelisted', 'category', 'cusips']]
    tinfo = tinfo.dropna()
    tinfo.drop_duplicates(inplace=True)
    tinfo = tinfo[tinfo.isdelisted=='N']
    tinfo = tinfo[['ticker', 'permaticker']]
    return tinfo

def save_ishares_etf_holdings(url, etfname):
    response = requests.post(url)
    csv_bytes = response.content
    str_file = io.StringIO(csv_bytes.decode('utf-8'), newline='\n')
    reader = csv.reader(str_file)
    data_array = []
    for row_list in reader:
        data_array.append(row_list)
    str_file.close()
    etf = pd.DataFrame(data_array)
    holdings_date = etf.iloc[1, 1]
    hdate = datetime.strptime(holdings_date, '%b %d, %Y').strftime('%Y-%m-%d')    
    etf = etf.dropna().reset_index(drop=True)
    etf.columns = etf.iloc[0,:]
    etf = etf[1:]
    etf.columns = etf.columns.str.lower()
    tinfo = get_tinfo()
    etfm = pd.merge(etf, tinfo, on=['ticker'], how='left')
    fpath = join(Path.home(), '.zipline\custom_data\constituents\{}'.format(etfname))
    if not os.path.isdir(fpath):
        os.makedirs(fpath)
    logging.info('Saving holdings for {}'.format(etfname))
    etfm.to_csv(join(fpath, '{}_{}.csv'.format(etfname, hdate)))

def download_spdr_holding_files():
    options = Options()
    options.add_argument("start-maximized")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://www.google.com")
    spdr_tickers = ['spy', 'xlb', 'xlc', 'xle', 'xlf', 'xli', 'xlk', 'xlp', 'xlre', 'xlu', 'xlv', 'xly']
    for ticker in spdr_tickers:
        url_spdr = r'https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{}.xlsx'.format(ticker)
        driver.get(url_spdr)

def save_spdr_etf_holdings():
    download_spdr_holding_files()
    tinfo = get_tinfo()
    download_path = join(Path.home(), 'Downloads')
    file_list = os.listdir(download_path)
    spdr_tickers = ['spy', 'xlb', 'xlc', 'xle', 'xlf', 'xli', 'xlk', 'xlp', 'xlre', 'xlu', 'xlv', 'xly']
    for ticker in spdr_tickers:
        spdr_list = [file for file in file_list if 'holdings-daily-us-en-{}'.format(ticker) in file]
        spdr_list_full = [join(download_path, spdr) for spdr in spdr_list]
        newest_file = max(spdr_list_full, key=os.path.getctime)
    
        df = pd.read_excel(newest_file)
        holdings_date = df.iloc[1,1].replace('As of ', "")
        hdate = datetime.strptime(holdings_date, '%d-%b-%Y').strftime('%Y-%m-%d')
        df = df.iloc[3:,:8]
        df.columns = df.iloc[0,:]
        df = df[1:]
        df = df.dropna()
        df.columns = df.columns.str.lower()
        dfm = pd.merge(df, tinfo, on='ticker', how='left')    
        fpath = join(Path.home(), '.zipline\custom_data\constituents\{}'.format(ticker))
        if not os.path.isdir(fpath):
            os.makedirs(fpath)
        logging.info('Saving holdings for {}'.format(ticker))
        dfm.to_csv(join(fpath, '{}_{}.csv'.format(ticker, hdate)))

def update_ishares_holdings(df, fdate):
    df.dropna(subset=['permaticker'], inplace=True)
    df['market value'] = df['market value'].str.replace(',', "").astype(float)
    df['weight'] = df['market value'] / df['market value'].sum()
    df.set_index('permaticker', drop=True, inplace=True)
    df = df[['weight']].T
    df.index = [pd.to_datetime(fdate)]
    df.columns.name = None
    return df
    
def update_spdr_holdings(df, fdate):
    df.dropna(subset=['permaticker'], inplace=True)
    df['weight'] = df['weight'].astype(float)
    df['weight_adj'] = df.weight / df.weight.sum()
    df.set_index('permaticker', drop=True, inplace=True)
    df = df[['weight_adj']].T
    df.index = [pd.to_datetime(fdate)]
    df.columns.name = None
    return df
    
def update_holdings():
    cpath = join(Path.home(), '.zipline/custom_data/constituents')
    cdir = os.listdir(cpath)
    etf_names = [f for f in cdir if '.py' not in f]
    for etf_name in etf_names:
        epath = join(cpath, etf_name)
        daily_files = os.listdir(epath)
        daily_file_paths = [join(epath, file) for file in daily_files]
        latest_file = max(daily_file_paths, key=os.path.getctime)
        fdate = latest_file.split('_')[2].split('.')[0]
        df = pd.read_csv(latest_file, index_col=0)
        if 'market value' in list(df.columns):
            df_out = update_ishares_holdings(df, fdate)
        else:
            df_out = update_spdr_holdings(df, fdate)
        fundamentals_path = join(Path.home(), '.zipline/data/fundamentals')
        holding_file_name = join(fundamentals_path, etf_name + '_constits.csv')
        logging.info('Updating holdings for {}'.format(etf_name))
        if os.path.isfile(holding_file_name):
            hf = pd.read_csv(holding_file_name, index_col=0)
            if fdate > hf.index.max():
                hf.columns = hf.columns.astype(float)
                hfm = pd.merge(hf.T, df_out.T, left_index=True, right_index=True, how='outer')
                hfm = hfm.T
                hfm.index = pd.to_datetime(hfm.index)
                hfm.to_csv(join(fundamentals_path, etf_name + '_constits.csv'))
        else:
            df_out.to_csv(join(fundamentals_path, etf_name + '_constits.csv'))

url_iwm = r'https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund'
url_iwb = r'https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund'
url_iwv = r'https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund'
save_ishares_etf_holdings(url_iwm, 'iwm')
save_ishares_etf_holdings(url_iwb, 'iwb')
save_ishares_etf_holdings(url_iwv, 'iwv')
save_spdr_etf_holdings()
update_holdings()
