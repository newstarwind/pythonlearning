import pyodbc as p
import pandas as pd

conn = p.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")

df = pd.read_sql('SELECT PricingWorker_HeartBeatUtc, PricingWorker_LastUpdateUtc, id \
  FROM dbo.PricingWorker_Heartbeat', con = conn)

print(df)

conn.close()




