from heapq import nsmallest

# 选出所有的总市值最小的N只股票
df = get_fundamentals(query(
        valuation.code, valuation.market_cap
    ), date='2014-01-01') #日期改为回测开始日期
df = df.dropna().sort(columns='market_cap',ascending=True)
df = df.head(300)
# 选取上面的结果作为universe
g.security = list(df['code'])
set_universe(g.security)
# 策略参考标准
set_benchmark('000300.XSHG')
# 设置手续费，买入时万分之三，卖出时万分之三加千分之一印花税, 每笔交易最低扣5块钱
set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
stocknum = 15 #购买股票数
g.count = 0
g.refresh_rate = 15 #调仓频率

def handle_data(context, data):
    # 止损
    for stock in list(context.portfolio.positions.keys()):
        his = history(2, '1d', 'close', [stock])
        if ((1-(his[stock][-1]/his[stock][0]))>=0.05):
            order_target(stock, 0)
    # 调仓日交易
    if g.count % g.refresh_rate == 0:
        # security = g.security
        # 去除流动性差的股票
        hist = history(20, '1d', 'volume', g.security)
        x =  hist.sum()
        x = x[x > 10**8]
        vw_list = list(x.keys())
        
        # 选出低股价的股票
        bucket = {}
        for stock in vw_list:
            hi = history(1, '1d', 'price', stock)
            bucket[stock] = hi[stock][-1]
        buylist = nsmallest(stocknum, bucket, key=bucket.get)
        
        # 目前持仓中不在buylist中的股票，清仓
        for stock in list(context.portfolio.positions.keys()):
            his = history(2, '1d', 'close', [stock])
            if stock not in buylist:
                order_target(stock, 0)
                
        # 等权重买入buylist中的股票
        position_per_stk = context.portfolio.cash/stocknum
        for stock in buylist:
            if not data[stock].isnan():
                amount = int(position_per_stk/data[stock].pre_close/100.0) * 100
                order(stock, +amount)
    # 天数加一
    g.count += 1 