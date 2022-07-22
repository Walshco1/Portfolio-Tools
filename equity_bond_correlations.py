
import pandas_datareader as pdr
import matplotlib.pyplot as plt

from pathlib import Path
from os.path import join

def get_spy_tlt_correlation():
    spy = pdr.data.DataReader('SPY', 'yahoo', '1993-01-28')['Adj Close']
    tlt = pdr.data.DataReader('TLT', 'yahoo', '2002-07-29')['Adj Close']
    # vxx = pdr.data.DataReader('VXX', 'yahoo', '2018-01-24')['Adj Close']
    # uvxy = pdr.data.DataReader('UVXY', 'yahoo', '2011-10-03')['Adj Close']

    spy_tlt_1m = spy.rolling(21).corr(tlt).dropna()
    spy_tlt_3m = spy.rolling(63).corr(tlt).dropna()
    spy_tlt_6m = spy.rolling(126).corr(tlt).dropna()
    spy_tlt_1y = spy.rolling(252).corr(tlt).dropna()

    fig, axs = plt.subplots(4, 3, figsize=(8.5, 11), gridspec_kw={'width_ratios': [6, 2, 1]})
    fig.autofmt_xdate(rotation=90)
    # First Row
    axs[0, 0].plot(spy_tlt_1m)
    axs[0, 0].set_title("SPY/TLT 1M Correlation")
    axs[0, 0].set_ylim(-1, 1)
    axs[0, 1].plot(spy_tlt_1m[-126:])
    axs[0, 1].set_ylim(-1, 1)
    
    axs[0, 2].boxplot(spy_tlt_1m)
    axs[0, 2].scatter(1, spy_tlt_1m[-1], marker='x', s=20, color='r')
    axs[0, 2].set_ylim(-1, 1)
    # Second Row
    axs[1, 0].plot(spy_tlt_3m)
    axs[1, 0].set_title("SPY/TLT 3M Correlation")
    axs[1, 0].set_ylim(-1, 1)
    axs[1, 1].plot(spy_tlt_3m[-126:])
    axs[1, 1].set_ylim(-1, 1)
    axs[1, 2].boxplot(spy_tlt_3m)
    axs[1, 2].scatter(1, spy_tlt_3m[-1], marker='x', s=20, color='r')
    axs[1, 2].set_ylim(-1, 1)
    # Third Row
    axs[2, 0].plot(spy_tlt_6m)
    axs[2, 0].set_title("SPY/TLT 6M Correlation")
    axs[2, 0].set_ylim(-1, 1)
    axs[2, 1].plot(spy_tlt_6m[-126:])
    axs[2, 1].set_ylim(-1, 1)
    axs[2, 2].boxplot(spy_tlt_6m)
    axs[2, 2].scatter(1, spy_tlt_6m[-1], marker='x', s=20, color='r')
    axs[2, 2].set_ylim(-1, 1)
    # Fourth Row
    axs[3, 0].plot(spy_tlt_1y)
    axs[3, 0].set_title("SPY/TLT 1Y Correlation")
    axs[3, 0].set_ylim(-1, 1)
    axs[3, 1].plot(spy_tlt_1y[-126:])
    axs[3, 1].set_ylim(-1, 1)
    axs[3, 2].boxplot(spy_tlt_1y)
    axs[3, 2].scatter(1, spy_tlt_1y[-1], marker='x', s=20, color='r')
    axs[3, 2].set_ylim(-1, 1)
    fig.tight_layout()
    
    # Save to file
    image_path = join(Path.home(), 'investments\daily_report\images')
    fig.savefig(join(image_path, 'spy_tlt_correlation.png'))
