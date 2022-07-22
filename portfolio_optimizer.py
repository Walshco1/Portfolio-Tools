
import cvxpy as cvx
from datetime import datetime
from datetime import timedelta
import numpy as np
import os
import pandas as pd
import pickle

import redis
import alpaca_trade_api as tradeapi
import matplotlib.pyplot as plt

redis_url_paper = "redis url"
r = redis.from_url(redis_url_paper)
loaded_state = pickle.loads(r.get('pylivetrader_redis_state'))
loaded_state.keys()

preds_df = loaded_state['preds_df']

l = preds_df['preds'].nlargest(40)
s = preds_df['preds'].nsmallest(40)
ret_preds = l.append(s)

def get_rets(symbol, ndays):
    
    today = datetime.today()
    current = today.strftime("%Y-%m-%d")
    cal_days = ndays*2
    past = (today - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    price_df = api.polygon.historic_agg_v2(symbol=symbol, 
                                 multiplier=1, 
                                 timespan='day',
                                 _from=past, 
                                 to=current).df
    price_rets = price_df.close.pct_change()
    price_rets = price_rets[-ndays:]
    return price_rets

rets_df = pd.DataFrame()
for security in ret_preds.index:
    rets_df[security] = get_rets(security.symbol, 25)



def optimize_portfolio(rdf, rpreds):
    
    # Define data
    mu = rpreds.values.reshape(len(rpreds), 1)
    sigma = rdf.cov().values
    n = len(sigma)
    
    # Define parameters
    w = cvx.Variable(n)
    gamma = cvx.Parameter(nonneg=True)
    
    # Define problem
    ret = mu.T*w
    risk = cvx.quad_form(w, sigma)
    objective = cvx.Maximize(ret - gamma*risk)
    constraints=[cvx.sum(w) == 0,
                 cvx.norm(w, 1) <= 1.0,
                 w >= -0.05,
                 w <= 0.05]
    prob = cvx.Problem(objective, constraints)
    
    # Run optimization
    SAMPLES = 100
    risk_data = np.zeros(SAMPLES)
    ret_data = np.zeros(SAMPLES)
    gamma_vals = np.logspace(-2, 3, num=SAMPLES)
    
    w_list = []
    for i in range(SAMPLES):
        gamma.value = gamma_vals[i]
        prob.solve(solver=cvx.SCS)
        w_list.append(w.value)
        risk_data[i] = cvx.sqrt(risk).value
        ret_data[i] = ret.value
    
    # Get optimal Portfolio
    sharpe = (ret_data-ret_data[-1])/risk_data
    #plt.plot(sharpe)
    wtidx = sharpe.argmax()
    pweights = w_list[wtidx]
    
    # Plot efficient frontier
    markers_on = [wtidx]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.plot(risk_data, ret_data, 'g-')
    for marker in markers_on:
        plt.plot(risk_data[marker], ret_data[marker], 'bs')
        ax.annotate(r"$\gamma = %.2f$" % gamma_vals[marker], 
                    xy=(risk_data[marker]+0.0002, 
                        ret_data[marker]-0.0005))
    plt.xlabel('Risk')
    plt.ylabel('Return')
    plt.show()
    
    odf = pd.DataFrame(rpreds)
    odf['wts'] = np.round(pweights,3)

    # Output longs and shorts
    longs = odf.wts.nlargest(15)
    shorts = odf.wts.nsmallest(15)
    return longs, shorts

long, short = optimize_portfolio(rets_df, ret_preds)

