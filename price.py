#coding:utf-8
import pyodbc as p
import pandas as pd
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")
sql = "select TOP 1 [open], [close],[time] from [TradeHero].[dbo].[KLine] where symbol = ? order by time desc"


def getPrice(symbol):
    try:
        conn = p.connect("DSN=MyCN;UID=tradehero_api;PWD=__sa90070104th__")
        df = pd.read_sql(sql , con = conn, params = [symbol] )
        return df.head(1)
    except Exception, e:
        print('no price of symbol :' + str(symbol))
        raise e
    finally:
        conn.close()    


def getOpen(symbol):
    df = getPrice(symbol)
    return df['open'][0]

def getClose(symbol):
    df = getPrice(symbol)
    return df['close'][0]

if __name__ == '__main__':
    print(getClose('000002'))