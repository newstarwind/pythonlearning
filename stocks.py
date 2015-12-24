# coding=utf-8
import tushare as ts
import pandas as pd
import numpy as np
import time
import traceback
import sys
import os
import sqlite3
import db
reload(sys)                         # encode problem resolving
sys.setdefaultencoding('utf-8')     # encode problem resolving

report_start_year = 1993
report_end_year = 2015

path = '/Users/david/GitHub/pythonlearning/stock_data/'
log_path = '/Users/david/GitHub/pythonlearning/stock_data/'

# path = 'H:\\stock_data\\'
# log_path = 'H:\\stock_data\\'

current_date = '2015-12-30'
download = []


def converSymbol(symbol):
    '将类型为数字的股票代码转化为字符类型的股票代码，比如 1 变成 000001'
    symbol = str(symbol)
    return symbol.rjust(6,'0')

def getDownloaded():
    for file in os.listdir(path):
        if file.endswith('.csv'):
            download.append(file)


def logError(s):
    f = open(log_path + 'log.txt', 'a')
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


def reportByYear(year, quarter):
    return str(year) + '-' + str(quarter) + '.csv'


def profitByYear(year, quarter):
    return str(year) + '-' + str(quarter) + '-profile' + '.csv'


def growthByYear(year, quarter):
    return str(year) + '-' + str(quarter) + '-growth' + '.csv'


def debtByYear(year, quarter):
    return str(year) + '-' + str(quarter) + '-debt' + '.csv'


def getQuerterReport(year):
    for q in [1, 2, 3, 4]:
        getDownloaded()
        if not isDownloaded(reportByYear(year, q)):
            try:
                print('Report of ' + str(year) + ' and quarter is ' + str(q))
                report_df = ts.get_report_data(year, q)
                report_df.to_csv(path + reportByYear(year, q))
                time.sleep(5)
            except Exception:
                logError('Report: ' + str(year) + '-' + str(q) +
                         ' there is problem, will skip it. ')


def getProfileReport(year):
    for q in [1, 2, 3, 4]:
        getDownloaded()
        if not isDownloaded(profitByYear(year, q)):
            try:
                print('Profit of ' + str(year) + ' and quarter is ' + str(q))
                profit_df = ts.get_profit_data(year, q)
                profit_df.to_csv(path + profitByYear(year, q))
                time.sleep(5)
            except Exception:
                logError('Profit: ' + str(year) + '-' + str(q) +
                         ' there is problem, will skip it. ')


def getGrowthReport(year):
    for q in [1, 2, 3, 4]:
        getDownloaded()
        if not isDownloaded(growthByYear(year, q)):
            try:
                print('Growth of ' + str(year) + ' and quarter is ' + str(q))
                growth_df = ts.get_growth_data(year, q)
                growth_df.to_csv(path + growthByYear(year, q))
                time.sleep(5)
            except Exception:
                logError('Growth: ' + str(year) + '-' + str(q) +
                         ' there is problem, will skip it. ')


def getDebtReport(year):
    for q in [1, 2, 3, 4]:
        getDownloaded()
        if not isDownloaded(debtByYear(year, q)):
            try:
                print('Debt of ' + str(year) + ' and quarter is ' + str(q))
                debt_df = ts.get_debtpaying_data(year, q)
                debt_df.to_csv(path + debtByYear(year, q))
                time.sleep(5)
            except Exception:
                logError('Debt: ' + str(year) + '-' + str(q) +
                         ' there is problem, will skip it. ')


def getReports(startYear, endYear):
    for year in range(startYear, endYear + 1):
        getQuerterReport(year)
        getProfileReport(year)
        getGrowthReport(year)
        getDebtReport(year)


def queryBasic(symbol):
    conn = sqlite3.connect(db.db_path)
    try:
        df = pd.read_sql('select * from ' + db.stock_basics +
                         ' where code = ?', con=conn, params=[symbol])
        return df
    except Exception, e:
        raise e
    finally:
        conn.close()

def getAllStocks():
    '返回以股票代码为索引的所有股票的名称，行业，上市时间'
    conn = sqlite3.connect(db.db_path)
    try:
        # df = pd.read_sql('select code, name, industry, timeToMarket from ' + db.stock_basics, con=conn, index_col = ['code'])
        df = pd.read_sql('select code, name, industry, timeToMarket from ' + db.stock_basics, con=conn)
        return df
    except Exception, e:
        raise e
    finally:
        conn.close()

def queryIndustry(symbol):
    df = queryBasic(symbol)
    return df['industry'][0]


if __name__ == '__main__':
    # getReports(report_start_year,report_end_year)
    # getStocks()

    # print(queryIndustry('0000001'))
    # print(getAllStocks())
    print(converSymbol(1))










