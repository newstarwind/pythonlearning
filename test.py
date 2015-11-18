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

path = '/Users/david/GitHub/pythonlearning/stock_data/'
log_path = '/Users/david/GitHub/pythonlearning/stock_data/'

# path = 'H:\\stock_data\\'
# log_path = 'H:\\stock_data\\'

current_date = '2015-12-30'
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


def logError(s):
    f = open(log_path + 'log.txt','a')            
    f.writelines(s)
    traceback.print_exc(file=f)
    f.flush()
    f.close()


def stringToDate(a):
    # 从df中拿到的日期不是字符串，是np.array[int]格式
    s = np.array2string(a)
    return s[:4] + '-' + s[4:6] + '-' + s[6:8]

def isDownloaded(name):
    return download.count(name) > 0

def nameByCode(code):
    return code + '.csv'

def getStockPrice(code, startDate, endDate):
    try:
        print(code + ' is published at ' + startDate + ' is in processing...')
        price_df = ts.get_h_data(code, start=startDate, end=endDate)
        price_df.to_csv(path + nameByCode(code))
        print('\n' + code + ' is done!')
    except Exception:
        logError('When dealing with code: ' + code +
              ' there is problem, will skip it. ')

def getStocks():
    df = ts.get_stock_basics()
    
    for code in df.index:
        getDownloaded()
        if not isDownloaded(nameByCode(code)):
            startDate = stringToDate(df.ix[code]['timeToMarket'])  # 上市日期
            getStockPrice(code, startDate, current_date)
            time.sleep(5)

# getStocks()



report_start_year = 1993
report_end_year = 2015

def reportByYear(year, quarter):
    return str(year) + '-' + str(quarter) + '.csv'



def getQuerterReport(year):
    for q in [1,2,3,4]:
        getDownloaded()
        if not isDownloaded(reportByYear(year,q)):
            try:
                print('Dealing withe report of ' + str(year) + ' and quarter is ' + str(q))
                report_df = ts.get_report_data(year,q)
                report_df.to_csv(path + reportByYear(year, q))
                time.sleep(5)
            except Exception:
                logError('When dealing with report: ' + str(year) + '-' + str(q) +
                ' there is problem, will skip it. ')

def getReports(startYear, endYear):
    for year in range(startYear, endYear + 1):
        getQuerterReport(year)
            
getReports(report_start_year,report_end_year)

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

