import numpy as np
import pandas as pd
from fpdf import FPDF
import dataframe_image as dfi
from pathlib import Path
from os.path import join

def modify_econ_cal_table(df):
    days = ['SUNDAY', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY']
    regstr = '|'.join(days)
    df['Time (ET)'].str.contains(regstr)
    df['date'] = df['Time (ET)']
    df['date'][~(df['date'].str.contains(regstr)==True)] = np.nan
    df['date'] = df['date'].ffill()
    df = df[(df['Time (ET)'].str.contains(regstr)==False)]
    df.insert(0, 'date', df.pop('date'))
    return df

def get_econ_cal_table():
    url = 'https://www.marketwatch.com/economy-politics/calendar'
    df1 = modify_econ_cal_table(pd.read_html(url, flavor='bs4')[0])
    df2 = modify_econ_cal_table(pd.read_html(url, flavor='bs4')[1])
    df = pd.concat([df1, df2])
    df.set_index('date', inplace=True)
    image_path = join(Path.home(), 'investments\daily_report\images')
    dfi.export(df, join(image_path, 'econ_cal.png'))
    return df
