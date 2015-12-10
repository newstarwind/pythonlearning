
#coding:utf-8
import pyodbc as p
import pandas as pd
import sys 
reload(sys) 
sys.setdefaultencoding("utf-8")

conn = p.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")



sql_main = "select * from dbo.FinancialMain where symbol = ? order by date desc"
sql_debt = "select * from dbo.FinancialDebt where symbol = ? order by date desc"
sql_cash = "select * from dbo.FinancialCash where symbol = ? order by date desc"
sql_profit = "select * from dbo.FinancialProfit where symbol = ? order by date desc"

# df = pd.read_sql('SELECT PricingWorker_HeartBeatUtc, PricingWorker_LastUpdateUtc, id \
#   FROM dbo.PricingWorker_Heartbeat', con = conn)

df_main = pd.read_sql(sql_main , con = conn, params = ['000002'] )

print(df_main.head(5))

conn.close()




