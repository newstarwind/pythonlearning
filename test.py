# coding=utf-8
import tushare as ts
import numpy as np
import pandas as pd

# print ts.__version__

# print(ts.get_hist_data('600848')) #获取600848当日行情信息
# print(ts.get_hist_data('600848',start='2015-01-05',end='2015-01-09')) #设定历史数据的时间

# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket'] #上市日期YYYYMMDD
# print(date)


#Select row by label
# data = ts.get_h_data('002337') #前复权
# day_data = data.loc['2015-09-15']
# print(day_data)
# print(data.info())


# Pandas.DataFrame
# one and two are likely columne name 
# a, b, c, d are likely the row label
# d = {'one' : pd.Series([1.,2.,3.],index = ['a','b','c']),
# 	 'two' : pd.Series([1.,2.,3.,4.], index = ['a','b','c','d'])}

# df = pd.DataFrame(d)
# print(df)
# df = pd.DataFrame(d, index = ['d','b','a'])
# print(df)

# 一次性获取当前交易所有股票的行情数据,如果是节假日，即为上一交易日
# ts.get_today_all()


