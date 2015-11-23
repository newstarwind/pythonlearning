import sqlite3
import pandas as pd
import stocks


db_path = '/Users/david/GitHub/pythonlearning/stock_data/stocks.db'
# db_path = 'H:\\stock_data\\stock.db'

table_name = 'stock_price'

conn = sqlite3.connect(db_path)
c = conn.cursor()


# c.execute('drop table stocks')

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

# # A saft way to insert records
# purchases = [('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
#              ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
#              ('2006-04-06', 'SELL', 'IBM', 500, 53.00)
#             ]
# c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)

# Read data from csv file
# df = pd.read_csv(stocks.path + '000001.csv')

# Change the columns and add 'code' in columns
# df['code'] = '000001'
# cols = df.columns.tolist()
# cols = cols[-1:] + cols[:-1]
# df = df[cols]

# Insert to db.stocks
# df.to_sql(table_name, conn, if_exists='append', index = False)

# Read data from sqlite to dataframe
df = pd.read_sql('select * from ' + table_name + ' order by date asc ', con = conn)

conn.commit()
conn.close()

print(df.tail(3))

