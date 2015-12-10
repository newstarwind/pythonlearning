# coding=utf-8
import sqlite3
import pandas as pd
import stocks
import sys
reload(sys)                         # encode problem resolving
sys.setdefaultencoding('utf-8')     # encode problem resolving


db_path = '/Users/david/GitHub/pythonlearning/stock_data/stocks.db'
# db_path = 'H:\\stock_data\\stock.db'

table_name = 'stock_price'
stock_basics = 'stock_basics'

# conn = sqlite3.connect(db_path)
# c = conn.cursor()

# Create the table in sqlite
# Stocks ['code','date', 'open', 'high', 'close', 'low', 'volume', 'amount']
# c.execute('''CREATE TABLE stock_price
#               (code     text,
#                date     numeric,
#                open     real,
#                high     real,
#                close    real,
#                low      real,
#                volume   real,
#                amount   real,
#                constraint pk_stocks_price primary key (code, date)
#                )''')

# A saft way to insert records
# purchases = [('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
#              ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
#              ('2006-04-06', 'SELL', 'IBM', 500, 53.00)
#             ]
# c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)


def readPrice(code):
    "Read data from csv file"
    df = pd.read_csv(stocks.path + stocks.nameByCode(code))
    return df


def addCodeColumn(df):
    "Change the columns and add 'code' as the first columns"
    df['code'] = '000001'
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]


def insertPrice(df, conn):
    'Insert to stocks_price'
    df.to_sql(table_name, conn, if_exists='append', index=False)


def queryCodePrice(code, conn):
    "Read price data from table 'stock_price' to dataframe, please notice the params must be set into '?' "
    df = pd.read_sql(
        'select * from ' + table_name + ' where code = ?', con=conn, params=[code])
    return df


def insertStockBasics():
    '将所有股票的基本信息写入本地数据库stock_basics表'
    try:
        df = pd.read_excel(stocks.path + 'stock_basics.xlsx')
        conn = sqlite3.connect(db_path)
        df.to_sql(stock_basics, conn, if_exists='replace', index=False)
    except Exception, e:
        raise e
    finally:
        conn.close()


if __name__ == '__main__':
    pass
    # conn = sqlite3.connect(db_path)
    # df = queryCodePrice('000001', conn)
    # print(df.tail(3))

    # insertStockBasics()
