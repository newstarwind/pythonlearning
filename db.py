# coding=utf-8
import sqlite3
import pandas as pd
import pyodbc as odbc
import stocks
import sys
from pandas import datetime
# from price import getPriceHistoryOf
reload(sys)                         # encode problem resolving
sys.setdefaultencoding('utf-8')     # encode problem resolving



db_path = '/Users/david/GitHub/pythonlearning/stock_data/stocks.db'
# db_path = 'H:\\stock_data\\stock.db'

stock_price = 'stock_price'  # 股价表
stock_basics = 'stock_basics'  # 所有股票的基本信息，包括代码，行业，上市时期
stock_meansures = 'stock_meansures'  # 计算得到的财务指标信息
stock_finance_reports = 'stock_finance_reports'  # 所有股票2000年后的财报，包括季报和年报
start_date = '2000-12-31'  # 财报开始年限
sql_server_connection = "DSN=MyCN;UID=tradehero_api;PWD=__sa90070104th__"
LOCAL = False   #是否使用本地数据库

#todo: 本地数据库价格不对，需要调整

def getConnection(local = False):
    if local: return sqlite3.connect(db_path)
    else: return odbc.connect(sql_server_connection)

def __addCodeColumn(df):
    "Change the columns and add 'code' as the first columns"
    df['code'] = '000001'
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    

def __insertPrice(df, conn):
    'Insert to stocks_price'
    df.to_sql(stock_price, conn, if_exists='append', index=False)


def insertMeasures2local(df):
    'Insert calculated measures into stock_meansures'
    conn = sqlite3.connect(db_path)
    df.to_sql(stock_meansures, conn, if_exists='replace', index_label='symbol')


def queryMeasureOf(symbol):
    'Read calculated measures from table stock_meansures'
    conn = sqlite3.connect(db_path)
    df = pd.read_sql('select * from ' + stock_meansures +
                     ' where symbol = ? ', con=conn, params=[symbol])
    return df


def queryMeasures():
    'Read all measures in local db'
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        'select * from ' + stock_meansures, con=conn, index_col='symbol')
    return df


def queryLocalPrice(code):
    "Read price data from table 'stock_price' to dataframe, please notice the params must be set into '?' "
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(
            'select * from ' + stock_price + ' where symbol = ?', con=conn, params=[code])
        return df
    except Exception, e:
        raise e
    finally:
        conn.close()    


def __insertStockBasics():
    '将所有股票的基本信息写入本地数据库stock_basics表'
    try:
        df = pd.read_excel(stocks.path + 'stock_basics.xlsx')
        conn = sqlite3.connect(db_path)
        df.to_sql(stock_basics, conn, if_exists='replace', index=False)
    except Exception, e:
        raise e
    finally:
        conn.close()

def __queryReportHistoryOf(symbol):
    'Query the history finance report'
    sql_main = 'select * from [TradeHero].[dbo].[FinancialMain]   where [symbol]= ? and [date] >= ? order by date desc'
    sql_debt = 'select * from [TradeHero].[dbo].[FinancialDebt]   where [symbol]= ? and [date] >= ? order by date desc'
    sql_profit = 'select * from [TradeHero].[dbo].[FinancialProfit] where [symbol]= ? and [date] >= ? order by date desc'
    sql_cash = 'select * from [TradeHero].[dbo].[FinancialCash]   where [symbol]= ? and [date] >= ? order by date desc'
    try:
        conn = odbc.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")
        main = pd.read_sql(sql_main,        con=conn, params=[symbol, start_date])
        debt = pd.read_sql(sql_debt,        con=conn, params=[symbol, start_date])
        profit = pd.read_sql(sql_profit,    con=conn, params=[symbol, start_date])
        cash = pd.read_sql(sql_cash,        con=conn, params=[symbol, start_date])
        # 删除冗余列
        del main['id']
        del debt['id']
        del profit['id']
        del cash['id']
        # 对四个数据集按照 symbol，data这个复合组建进行合并
        result = pd.merge(pd.merge(pd.merge(main, debt, on=['symbol', 'date']), profit, on=[
                          'symbol', 'date']), cash, on=['symbol', 'date'])
        return result
    except Exception, e:
        raise e
    finally:
        conn.close()


def querylocalReportsOf(symbol):
    sql = 'select * from ' + stock_finance_reports + ' where symbol = ? '
    try:
        conn = sqlite3.connect(db_path)
        return pd.read_sql(sql, con=conn, params=[symbol])
    except Exception, e:
        raise e
    finally:
        conn.close()


def querylocalReportAt(symbol, date):
    sql = 'select * from ' + stock_finance_reports + \
        ' where symbol = ? and date = ? '
    try:
        conn = sqlite3.connect(db_path)
        return pd.read_sql(sql, con=conn, params=[symbol, date])
    except Exception, e:
        raise e
    finally:
        conn.close()


def __insertHistoryReports2local(symbol):
    try:
        conn = sqlite3.connect(db_path)
        df = __queryReportHistoryOf(symbol)
        df.to_sql(stock_finance_reports, conn, if_exists='append', index=False)
    except Exception, e:
        raise e
    finally:
        conn.close()


def __insertAllHistoryReport2local():
    allstocks = stocks.getAllStocks()
    for row in allstocks.iterrows():
        symbol = stocks.converSymbol(row[1][0])
        name = row[1][1]
        print('Processing: ' + str(symbol) + name)
        __insertHistoryReports2local(symbol)

def getPriceHistoryOf(symbol):
    conn = getConnection(local = False)
    sql = 'SELECT [symbol], [time], [open], [close], [volume] FROM [TradeHero].[dbo].[KLine] where symbol = ? and tag= 1 order by time desc'
    df = pd.read_sql(sql, conn, params = [symbol])
    conn.close()
    return df

def insertPriceHistoryOf(symbol):        
    conn = sqlite3.connect(db_path)
    try:
        df = getPriceHistoryOf(symbol)
        df.to_sql(stock_price, conn, if_exists='append', index=False)
    except Exception, e:
        raise e
    finally:
        conn.close()

def insertAllPrice():
    allstocks = stocks.getAllStocks()
    for row in allstocks.iterrows():
        symbol = stocks.converSymbol(row[1][0])
        name = row[1][1]
        print('Inserting: ' + str(symbol) + name)
        insertPriceHistoryOf(symbol)


def queryLocalData(table):
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql('select count(*) from ' + table, conn)
        conn.close()
        print(df)
    except Exception, e:
        raise e
    finally:
        conn.close()

def dropLocalTable(table):        
    try:
        conn = sqlite3.connect(db_path)
        pd.read_sql('drop table ' + table, conn)
    except Exception, e:
        raise e
    finally:
        conn.close()


if __name__ == '__main__':
    # __insertStockBasics()
    # __insertAllHistoryReport2local()

    # print(queryMeasureOf('000002'))
    # print() __queryMeasures())

    # print(querylocalReportsOf('000905'))
    # print(querylocalReportAt('000905', '2001-12-31'))

    # print(queryMeasureOf('000905'))

    # print(queryLocalPrice('000002'))

    queryLocalData(stock_price)

    # print(getPriceHistoryOf('000002'))

    # dropLocalTable(stock_price)

    # insertAllPrice()  
    # pass
