# coding:utf-8
import pyodbc as p
import pandas as pd
import sys
import price
import stocks
import traceback
from pandas import Series
from db import insertMeasures2local
from db import queryMeasures
from db import querylocalReportsOf
from db import start_date
from db import LOCAL
from pandas import datetime

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
# 杠杆率 Leverage: 总资产 / 股东权益                                          【debt_11/debt_24】
# EV：市场价值 + 负债 - 减去现金                                              【debt_58*price + debt_79 - debt_1】
# EBIT: 净利润＋所得税＋利息                                                  【profit_15 + profit_14 + profit_2】
# EBITDA = EBIT + 折旧 + 摊销
# 自由现金流(Free Cash Flow, FCF), Capex
# FCF = 营运现金流(cash_7) - Interest(profit_2) - Tax(profit_24) - Capex (cash_20)
# 现金收益率: 自由现金流 / EV
# 清理哪些是累计值，哪些是当前值，比如EPS是季度累计值，每个季度往后累加，直至年度 todo

def init(symbol):
    global REPORTS
    try:
        REPORTS = querylocalReportsOf(symbol)
    except Exception, e:
        raise e

def getReportsBy(date):
    global REPORTS
    return REPORTS[REPORTS.date == date]

def getSize(symbol, date):
    '当前市值 = 总股本数 * 股价'
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol,date, local = LOCAL)
    amount = df['debt_58'].iloc[0] * price_close
    return amount

def getSize4Bank(symbol, date):
    '对于银行股，使用debt_20作为总股本数，而不是debt_58'
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol,date, local = LOCAL)
    amount = df['debt_20'].iloc[0] * price_close
    return amount

def getEPS(symbol,date):
    '每股收益'
    df = getReportsBy(date)
    eps = df['main_1'].iloc[0]
    return eps


def getPE(symbol,date):
    '市盈率 PE = Price / 每股收益'
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol,date, local = LOCAL)
    print('price is : ' + str(price_close))
    pe = price_close / df['main_1'].iloc[0]
    print("eps is : " + str(df['main_1'].iloc[0]))
    return pe

def getPB(symbol,date):    
    '市净率 PB = price / 每股净资产'
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol, date, local = LOCAL)
    pb = price_close / df['main_7'].iloc[0]
    return pb

def getPS(symbol,date):
    '市销率 PS = 当前市值 / 营业收入'
    df = getReportsBy(date)
    size = getSize(symbol,date)
    ps = size / df['main_5'].iloc[0]
    return ps

def getPEG(symbol, date):
    'PEG = PE / EPS增长率'
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol, date, local = LOCAL)
    pe = getPE(symbol,date)
    peg = pe / df['main_14'].iloc[0]
    return peg

def getROA(symbol, date):
    df = getReportsBy(date)
    total_capatal = polishValue(df['debt_24'].iloc[0])    \
                    + polishValue(df['debt_41'].iloc[0])  \
                    + polishValue(df['debt_50'].iloc[0])
    roa = df['profit_15'].iloc[0] / total_capatal
    return roa


def getROE(symbol,date):
    df = getReportsBy(date)
    roe = df['profit_15'].iloc[0] / df['debt_24'].iloc[0]
    return roe


def getEV(symbol,date):
    df = getReportsBy(date)
    price_close = price.getCloseAt(symbol,date, local = LOCAL)
    ev = polishValue(df['debt_58'].iloc[0]) * price_close + \
        polishValue(df['debt_79'].iloc[0]) - polishValue(df['debt_1'].iloc[0])
    return ev


def getEBIT(symbol,date):
    df = getReportsBy(date)
    ebit = polishValue(df['profit_15'].iloc[0])   \
            + polishValue(df['profit_14'].iloc[0])  \
            + polishValue(df['profit_2'].iloc[0])
    return ebit


def getEBIT_EV(symbol,date):
    ebit_ev = getEBIT(symbol,date) / getEV(symbol,date)
    return ebit_ev


def getLeverage(symbol,date):
    '财务杠杆：负债 / 股东权益'
    df = getReportsBy(date)
    leverage = polishValue(df['debt_11'].iloc[0]) / polishValue(df['debt_24'].iloc[0])
    return leverage

def getFCF(symbol,date):    
    'FCF = 营运现金流(cash_7) - 利息支出(profit_2) - 税费支出(profit_24) - Capex (cash_10)'
    df = getReportsBy(date)
    fcf = polishValue(df['cash_7'].iloc[0])     \
            - polishValue(df['profit_2'].iloc[0])  \
            - polishValue(df['profit_24'].iloc[0]) \
            - polishValue(df['cash_20'].iloc[0])
    return fcf

def getCRR(symbol,date):
    '现金收益率: 自由现金流 / EV'    
    df = getReportsBy(date)
    fcf = getFCF(symbol,date)
    ev = getEV(symbol,date)
    crr = fcf / ev
    return crr

def polishValue(var):
    if var is None:
        var = 0
    return var


def getReportDateList():
    year_now = datetime.now().year.numerator
    year_start  = int(start_date[0:4])
    date_list = []
    while year_start < year_now:
        date = str(year_start) + '-12-31'
        year_start += 1
        date_list.append(date)
    return date_list

def getReportQuartList():
    year_now = datetime.now().year.numerator
    month_now = datetime.now().month.numerator
    year_start  = int(start_date[0:4])
    date_list = []
    while year_start < year_now:
        date_4 = str(year_start) + '-12-31'    
        date_3 = str(year_start) + '-09-30'
        date_2 = str(year_start) + '-06-30'
        date_1 = str(year_start) + '-03-31'
        year_start += 1
        date_list.append(date_1)
        date_list.append(date_2)
        date_list.append(date_3)
        date_list.append(date_4)
    if(month_now > 4): date_list.append(str(year_now) + '-03-31')     
    if(month_now > 7): date_list.append(str(year_now) + '-06-30')
    if(month_now > 10): date_list.append(str(year_now) + '-09-30')

    return date_list

def calYearOf(symbol,date):
    try:
        if stocks.queryIndustry(symbol) != '银行':
            init(symbol)
            measure = Series({
                            'symbol':       symbol,
                            'price':        price.getCloseAt(symbol,date),
                            'eps':          getEPS(symbol,date),
                            'peg':          getPEG(symbol,date),
                            'roa':          getROA(symbol,date),
                            'pe':           getPE(symbol,date),
                            'pb':           getPB(symbol,date),
                            'ps':           getPS(symbol,date),
                            'ev':           getEV(symbol,date),
                            'ebit':         getEBIT(symbol,date),
                            'fcf':          getFCF(symbol,date),
                            'crr':          getCRR(symbol,date),
                            'leverage':     getLeverage(symbol,date),
                            'ebit_ev':      getEBIT_EV(symbol,date)}, 
                            index=['symbol','price','eps','pe','pb','ps','peg','crr','leverage','ebit_ev'])
            return measure
    except Exception, e:
        traceback.print_exc()
        raise e

def calHistoryOf(symbol,byQuart = False):
    date_list = getReportDateList()
    if byQuart: date_list = getReportQuartList()
    measures = {}
    for date in date_list:
        try:
            print('Dealing with : ' + symbol + ' in ' + date)
            measure = calYearOf(symbol,date)
            measures[date] = measure
        except Exception, e:
            print('no data from date: ' + date)
            # pass
    result = pd.DataFrame(measures).T #行变列转置
    result.index.name = 'date'
    return result        

# def insertMeasures(num):
#     df = stocks.getAllStocks()
#     measures = {}
#     result = pd.DataFrame
#     count = 0
#     for row in df.iterrows():
#         name = row[1][1]
#         symbol = stocks.converSymbol(row[1][0])
#         count = count + 1
#         if count > num : break
#         try:
#             if stocks.queryIndustry(symbol) != '银行':
#                 init(symbol)
#                 print(name)
#                 measure = Series({
#                             'price':        price.getClose(symbol),
#                             'peg':          getPEG(symbol),
#                             'roa':          getROA(symbol),
#                             'pe':           getPE(symbol),
#                             'pb':           getPB(symbol),
#                             'ps':           getPS(symbol),
#                             'ev':           getEV(symbol),
#                             'ebit':         getEBIT(symbol),
#                             'leverage':     getLeverage(symbol),
#                             'ebit_ev':      getEBIT_EV(symbol)}, 
#                             index=['price','pe','pb','ps','roa','peg','ev','ebit','leverage','ebit_ev'])
#                 measures[symbol] = measure
#                 print('----------------------------')
#             else:
#                 print('跳过银行： ' + str(name) )
#                 print('----------------------------')
#         except Exception, e:
#             traceback.print_exc()
#             pass
#     result = pd.DataFrame(measures).T #行变列转置
#     result.index.name = 'symbol'
#     result = result.sort_values(by=['ebit_ev'], ascending=False) 
#     print(result)
#     insertMeasures2local(result)
   

if __name__ == '__main__':
    print(calHistoryOf('000002',byQuart = False))
    
    # global LOCAL
    # init('000002')
    # print(getPE('000002','2004-12-31'))
    # print(LOCAL)
    # LOCAL = False
    # print(getPE('000002','2004-12-31'))
    # print(LOCAL)

