#coding:utf-8
import pandas as pd
from db import stock_price
from db import db_path
from db import getConnection
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")

sql = "select TOP 1 [open], [close],[time] from [TradeHero].[dbo].[KLine] where symbol = ? order by time desc"
sql_local = "select open, close, time from "+ stock_price +" where symbol = ? order by time desc limit 1"
sql_priceat = "select TOP 1 [open], [close],[time] from [TradeHero].[dbo].[KLine] where tag = 1 and symbol = ? and time <= ? order by time desc"
sql_local_price_at = "select open, close,time from "+ stock_price +" where symbol = ? and time <= ? order by time desc limit 1"


def getPrice(symbol, local = False):
    conn = getConnection(local)
    try:
        if local: df = pd.read_sql(sql_local , con = conn, params = [symbol] )
        else: df =  pd.read_sql(sql, con = conn, params = [symbol])
        return df.head(1)
    except Exception, e:
        print('no price of symbol :' + str(symbol))
        raise e
    finally:
        conn.close()

def getPriceAt(symbol, date, local = False):
    '获取某只股票某日期的行情'
    conn = getConnection(local)
    try:
        if local: df = pd.read_sql(sql_local_price_at, con = conn, params = [symbol, date + ' 00:00:00'])
        else: df = pd.read_sql(sql_priceat, con = conn, params = [symbol, date])
        return df.head(1)
    except Exception, e:
        print('no price of symbol :' + str(symbol))
        raise e
    finally:
        conn.close()    

def getCloseAt(symbol, date, local = False):
    df = getPriceAt(symbol, date, local)
    return df['close'][0]

def getOpen(symbol, local = False):
    df = getPrice(symbol, local)
    return df['open'][0]

def getClose(symbol, local = False):
    df = getPrice(symbol, local)
    return df['close'][0]

if __name__ == '__main__':
    # print(getClose('000002'))
    # print(getCloseAt('000905','2000-12-31'))

    # print(getClose('000002',local = True))
    print(getCloseAt('000002', '2002-12-31', local = True))
    print(getCloseAt('000002', '2002-12-31', local = False))

    # conn = getConnection(local = True)
    # sql = 'select * from stock_price where symbol = ? and time <= ? order by time desc'
    # df = pd.read_sql(sql, conn, params = ['000002','2002-12-31 00:00:00'])
    # print(df)
    # conn.close()
