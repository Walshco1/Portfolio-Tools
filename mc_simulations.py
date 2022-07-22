from os.path import join
from pathlib import Path
import pandas as pd
import numpy as np
from statsmodels.stats.moment_helpers import corr2cov
import scipy.linalg

# Read in data
file_path = join(Path.home(), 'documents')
a_path = join(file_path, 'assumptions.csv')
assumptions = pd.read_csv(a_path, index_col=0)

# Adjust naming conventions for asset classes
assumptions.index = assumptions.index.str.replace("'", "")
assumptions.index = assumptions.index.str.replace("/", "-")
assumptions.columns = ['Return', 'Standard Deviation'] + list(assumptions.index)

# Create separate dataframes for return/risk and correlations
retrisk = assumptions[['Return', 'Standard Deviation']] / 100
retrisk.columns = ['ret', 'stdev']
correlations = assumptions[assumptions.index]

# Define which asset classes to include
asset_classes = [
    'Broad US Equity', 
    'Devd Large-Mid Intl Equity', 
    'Emerging Markets Equity',
    'Global Equity',        
    'US Agg Fixed Income',
    'Bank Loans',
    'Private Credit',
    'Private Equity',
    'Real Estate']

def reduce_assets(retrisk, correlations, asset_classes):
    rr = retrisk[retrisk.index.isin(asset_classes)].reindex(asset_classes)
    c = correlations[correlations.index.isin(asset_classes)].reindex(asset_classes)
    c = c[c.index]
    return rr, c
rr, c = reduce_assets(retrisk, correlations, asset_classes)

# Get annual input arrays
mu = rr.ret.values
sigma = rr.stdev.values
corr = c.values
cov = corr2cov(corr, sigma)

# Get quarterly input arrays
mu_q = (1+mu)**(1/4) - 1
sigma_q = sigma / np.sqrt(4)
cov_q = cov / 4

# Get quarterly input arrays
mu_m = (1+mu)**(1/12) - 1
sigma_m = sigma / np.sqrt(12)
cov_m = cov / 12

# Calculate correlated rets for 1 period
L = scipy.linalg.lu(corr)[1]
Z = np.random.normal(scale=sigma, size=len(sigma)) # assumes normality here
cor_var = np.inner(L, Z)
rets = mu + cor_var
print(pd.DataFrame(data={'rets': rets, 'mu': mu, 'sigma': sigma}, index=rr.index))


