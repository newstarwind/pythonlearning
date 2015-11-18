# coding=utf-8
import tushare as ts
import pandas as pd
import numpy as np
import time
import traceback
import sys
import os
reload(sys)                         # encode problem resolving
sys.setdefaultencoding('utf-8')     # encode problem resolving

# print ts.__version__
# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket']  # 上市日期YYYYMMDD
# print(date)

path = '/Users/david/GitHub/pythonlearning/stock_data/'
log_path = '/Users/david/GitHub/pythonlearning/stock_data/'

# path = 'H:\\stock_data\\'
# log_path = 'H:\\stock_data\\'

current_date = '2015-12-30'
sample_code = ['600350', '600848']
download = []

# def getAllDownloaded():
#     f = open(log_path + 'download.txt', 'r')
#     for line in f:
#         download.append(line.strip().lstrip().rstrip())
#     f.close()
#     print(download)

def getDownloaded():
    for file in os.listdir(path):
        if file.endswith('.csv'):
            download.append(file) 

getDownloaded()


def stringToDate(a):
    # 从df中拿到的日期不是字符串，是np.array[int]格式
    s = np.array2string(a)
    return s[:4] + '-' + s[4:6] + '-' + s[6:8]


def isDownloaded(code):
    return download.count(code) > 0

def nameByCode(code):
    return code + '.csv'

def getStockPrice(code, startDate, endDate):
    try:
        print(code + ' is published at ' + startDate + ' is in processing...')
        price_df = ts.get_h_data(code, start=startDate, end=endDate)
        price_df.to_csv(path + nameByCode(code))
        print('\n' + code + ' is done!')
    except Exception:
        print('When dealing with code: ' + code +
              ' there is problem, will skip it. ')
        f = open(log_path + 'log.txt', 'a')
        traceback.print_exc(file=f)
        f.flush()
        f.close()

def getStocks():
    df = ts.get_stock_basics()
    
    for code in df.index:
        getDownloaded()
        if not isDownloaded(nameByCode(code)):
            startDate = stringToDate(df.ix[code]['timeToMarket'])  # 上市日期
            getStockPrice(code, startDate, current_date)
            time.sleep(5)

getStocks()


# Get Stock price data
# df = ts.get_h_data(code,start='2002-01-01',end='2015-12-30')
# df.to_excel(path + code + '.xlsx')


# Get quarterly report data for all stocks
# df = ts.get_report_data(2014,3)
# df.to_excel(path + '2014-3.xlsx')

# Select row by label
# data = ts.get_h_data('002337') #前复权

# day_data = data.loc['2015-09-15']
# print(day_data)
# print(data.info())

