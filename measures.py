# coding:utf-8
import pyodbc as p
import pandas as pd
import sys
import price
import stocks

reload(sys)
sys.setdefaultencoding("utf-8")

# 当前股价 price
# 当前市值 size                                                               【debt_58 * price】
# 市盈率: P/E, 由股价除以年度每股盈余（EPS）得出， 越低越好。                 【price / main_1】
# 市净率: P/B, 每股股价与每股净资产的比率, 越低越好。                         【price/ main_7】
# 市销率: P/S,  总市值除以营业收入， 越低越好.                                【size/ main_5】
# EPS: 每股盈余， 越高越好。                                                  【main_1】
# 每股收益同比增长率：EPS增长率                                               【main_14】
# PEG(市盈率增长率比): 市盈率（PE）/EPS增长率                                 【（price / main_1）/ main_14】
# 总资产（Total Capital）= 股东权益 + 有息负债                                【debt_24 + debt_41 + debt_50】
# Total Capital = Long-Term Debt + Short-Term Debt + Equity
# 总资产回报率（ROA，ROI， ROIC， ROTC) = 效率 × 效益 = 净利润/总资产         【profit_15/(debt_24 + debt_41 + debt_50)】
# 净资产收益率（ROE）= Net Profit / Equity                                    【profit_15/debt_24】
# 营业收入(Operating Revenue)【在利润表上可以直接查到】                       【profit_5】
# 营业利润(销售利润) Sale Profit： 【在利润表上可以查到】                     【profit_7】
# EV：市场价值 + 负债 - 减去现金                                              【debt_58*price + debt_79 - debt_1】
# EBIT: 净利润＋所得税＋利息                                                  【profit_15 + profit_14 + profit_2】
# EBITDA = EBIT + 折旧 + 摊销

sql_debt = "select top 1 * from dbo.FinancialDebt where symbol = ? order by date desc"
sql_main = 'select top 1 * from dbo.FinancialMain where symbol = ? order by date desc'
sql_profit = 'select top 1 * from dbo.FinancialProfit where symbol = ? order by date desc'


def setDebt(symbol):
    global DEBT
    try:
        conn = p.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")
        DEBT = pd.read_sql(sql_debt, con=conn, params=[symbol])
    except Exception, e:
        raise e
    finally:
        conn.close()


def setMain(symbol):
    global MAIN
    try:
        conn = p.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")
        MAIN = pd.read_sql(sql_main, con=conn, params=[symbol])
    except Exception, e:
        raise e
    finally:
        conn.close()


def setProfit(symbol):
    global PROFIT
    try:
        conn = p.connect("DSN=MyDB;UID=tradehero_api;PWD=__sa90070104th__")
        PROFIT = pd.read_sql(sql_profit, con=conn, params=[symbol])
    except Exception, e:
        raise e
    finally:
        conn.close()


def init(symbol):
    setMain(symbol)
    setDebt(symbol)
    setProfit(symbol)


def getSize(symbol):
    global DEBT
    price_close = price.getClose(symbol)
    amount = DEBT['debt_58'][0] * price_close
    return amount


def getSize4Bank(symbol):
    '对于银行股，使用debt_20作为总股本数，而不是debt_58'
    global DEBT
    price_close = price.getClose(symbol)
    amount = DEBT['debt_20'][0] * price_close
    return amount


def getPE(symbol):
    global MAIN
    price_close = price.getClose(symbol)
    pe = price_close / MAIN['main_1'][0]
    return pe


def getPB(symbol):
    global MAIN
    price_close = price.getClose(symbol)
    pb = price_close / MAIN['main_7'][0]
    return pb


def getPS(symbol):
    global MAIN
    size = getSize(symbol)
    ps = size / MAIN['main_5'][0]
    return ps


def getPEG(symbol):
    global MAIN
    price_close = price.getClose(symbol)
    pe = getPE(symbol)
    peg = pe / MAIN['main_14'][0]
    return peg


def getROA(symbol):
    global MAIN, PROFIT, DEBT
    total_capatal = polishValue(DEBT['debt_24'][
                                0]) + polishValue(DEBT['debt_41'][0]) + polishValue(DEBT['debt_50'][0])
    roa = PROFIT['profit_15'][0] / total_capatal
    return roa


def getROE(symbol):
    global MAIN, PROFIT, DEBT
    roe = PROFIT['profit_15'][0] / DEBT['debt_24'][0]
    return roe


def getEV(symbol):
    global MAIN, PROFIT, DEBT
    price_close = price.getClose(symbol)
    ev = polishValue(DEBT['debt_58'][0]) * price_close + \
        polishValue(DEBT['debt_79'][0]) - polishValue(DEBT['debt_1'][0])
    return ev


def getEBIT(symbol):
    global MAIN, PROFIT, DEBT
    ebit = polishValue(PROFIT['profit_15'][
                       0]) + polishValue(PROFIT['profit_14'][0]) + polishValue(PROFIT['profit_2'][0])
    return ebit


def polishValue(var):
    if var is None:
        var = 0
    return var

if __name__ == '__main__':
 
    df = stocks.getAllStocks()
    count = 1

    for row in df.iterrows():
        name = row[1][1]
        symbol = row[1][0]
        symbol = stocks.converSymbol(symbol)
        try:
            if stocks.queryIndustry(symbol) != '银行':
                init(symbol)
                print('股票代码 ：' + symbol)
                print('PEG  of  '     + str(name) + ' : ' + str(getPEG(symbol)))
                print('ROA  of  '     + str(name) + ' : ' + str(getROA(symbol)))
                print('PE   of  '     + str(name) + ' : ' + str(getPE(symbol)))
                print('PB   of  '     + str(name) + ' : ' + str(getPB(symbol)))
                print('PS   of  '     + str(name) + ' : ' + str(getPS(symbol)))
                print('EV   of  '     + str(name) + ' : ' + str(getEV(symbol))   + '万元')
                print('EBIT of  '     + str(name) + ' : ' + str(getEBIT(symbol)) + '万元')
                print('----------------------------------------------------------------')
                count = count + 1
                if count > 200 : break
            else:
                print(str(name) + '是一家银行')
                print('----------------------------------------------------------------')
        except Exception, e:
            print(e)
        finally:
            pass

    

