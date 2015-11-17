# coding=utf-8
import tushare as ts
import pandas as pd
import numpy as np
import time
import sys
reload(sys)                         # encode problem resolving
sys.setdefaultencoding('utf-8')     # encode problem resolving

# print ts.__version__
# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket']  # 上市日期YYYYMMDD
# print(date)

path = '/Users/david/GitHub/pythonlearning/stock_data/'
current_date = '2015-12-30'
sample_code = ['000001', '600350', '600848']

def stringToDate(a):
    # 从df中拿到的日期不是字符串，是np.array[int]格式
    s = np.array2string(a)
    return s[:4] + '-' + s[4:6] + '-' + s[6:]

def getStockPrice(code, startDate, endDate):
    try:
        print(code + ' is published at ' + startDate + ' is in processing...')
        price_df = ts.get_h_data(code, start = startDate, end = endDate)
        price_df.to_excel(path + code + '.xlsx')
        print('\n' + code + ' is done!')
    except Exception:
        print('When dealing with code: ' + code + ' there is problem, will skip it. ')
        f = open(path + 'log.txt', 'a')
        traceback.print_exc(file = f)
        f.flush()
        f.close()

def getStocks():
    df = ts.get_stock_basics()
    for code in df.index:
        startDate = stringToDate(df.ix[code]['timeToMarket'])  # 上市日期
        getStockPrice(code, startDate, current_date )
        time.sleep(5)

getStocks()


# Get Stock price data
# df = ts.get_h_data(code,start='2002-01-01',end='2015-12-30')
# df.to_excel(path + code + '.xlsx')


# Get quarterly report data for all stocks
# df = ts.get_report_data(2014,3)
# df.to_excel(path + '2014-3.xlsx')


# print(ts.get_hist_data('600848')) //获取600848当日行情信息
# print(ts.get_hist_data('600848',start='2015-01-05',end='2015-01-09'))
# //设定历史数据的时间


# Select row by label
# data = ts.get_h_data('002337') #前复权

# day_data = data.loc['2015-09-15']
# print(day_data)
# print(data.info())


# Pandas.DataFrame
# one and two are likely columne name

# a, b, c, d are likely the row label
# d = {'one': pd.Series([1., 2., 3.], index=['a', 'b', 'c']),
#      'two': pd.Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
# print(d)

# df = pd.DataFrame(d)
# print(df)
# df = pd.DataFrame(d, index = ['d','b','a'])
# print(df)

# 一次性获取当前交易所有股票的行情数据,如果是节假日，即为上一交易日
# ts.get_today_all()
