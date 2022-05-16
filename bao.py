import baostock as bs
import pandas as pd
import math,datetime,requests,sys,re,json,time,random,pandas,hashlib, sklearn, numpy
from bs4 import BeautifulSoup
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from tool import *
from model.connecter import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed

class BaoStockImport(object):

    baostock = None
    def __init__(self):
        self.baostock = bs
        lg = self.baostock.login()
        # 显示登陆返回信息
        print('login respond error_code:'+lg.error_code)
        print('login respond  error_msg:'+lg.error_msg)

    def  __exit__(self, exc_type, exc_val, exc_tb):
        self.baostock.logout()

    def save_stock_info(self, stock):
        with DataBase() as db:
            query_list = [
                StockInfo.date == (datetime.datetime.now()-datetime.timedelta(days=3)).strftime('%Y-%m-%d'),
                StockInfo.code == stock["code"],
            ]
            if db.session.query(StockInfo).filter(*query_list).first():
                print ("跳过",stock["name"])
                return False
            price_list = []
            volume_list = [1]
            bao_data = self.baostock.query_history_k_data_plus(stock["market"] + "." + stock["code"], "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST", start_date="2021-10-01",  frequency="d", adjustflag="2").get_data()#查询股票日 k 线数据
            stock_info_list = list(bao_data.iterrows())
            for i, v in stock_info_list:
                db_data = db.session.query(StockInfo).filter(*[
                    StockInfo.date == v["date"],
                    StockInfo.code == stock["code"],
                    ]).first()
                if db_data:
                    print("日期跳过",stock["name"], v["date"])
                    stock_info_list[i] = db_data
                    continue
                data = StockInfo(
                    date = v[0],
                    open = decimal.Decimal(v[2]),
                    high = decimal.Decimal(v[3]),
                    low = decimal.Decimal(v[4]),
                    close = decimal.Decimal(v[5]),
                    preclose = decimal.Decimal(v[6]),
                    volume = int(v[7] if v[7] != "" else 0),
                    amount = decimal.Decimal(v[8] if v[8] != "" else 0),
                    adjustflag = int(v[9]),
                    turn = decimal.Decimal(v[10]) if v[10] != "" else 0,
                    tradestatus = int(v[11]),
                    pct_chg = decimal.Decimal(v[12]) if v[12] != "" else 0,
                    pre_pct_chg = 0,
                    pe_ttm = decimal.Decimal(v[13]) if v[13] != "" else 0,
                    ps_ttm = decimal.Decimal(v[14]) if v[14] != "" else 0,
                    pcf_ncf_ttm = decimal.Decimal(v[15]) if v[15] != "" else 0,
                    pb_mrq = decimal.Decimal(v[16]) if v[16] != "" else 0,
                    is_st = int(v[17]),
                )
                price_list.append(data.close)
                volume_list.append(data.volume if data.volume > 0 else 1)
                data.code = stock["code"]
                data.name = stock["name"]
                data.market = stock["market"]
                data.industry = stock["industry"]
                data.amplitude = (data.high-data.low)/data.preclose*100
                data.volume_rate = round(int(data.volume)/(sum(volume_list[-6:-1])/5), 2)
                data.avp120 = sum(map(float,price_list[-120:]))/120
                data.avp120_chg = data.avp120 - sum(map(float,price_list[-121:-1]))/120
                data.avp60 = sum(map(float,price_list[-60:]))/60
                data.avp60_chg = data.avp60 -sum(map(float,price_list[-61:-1]))/60
                data.avp30 = sum(map(float,price_list[-30:]))/30
                data.avp30_chg = data.avp30 - sum(map(float,price_list[-31:-1]))/30
                data.avp20 = sum(map(float,price_list[-20:]))/20
                data.avp20_chg = data.avp20 - sum(map(float,price_list[-21:-1]))/20
                data.avp10 = sum(map(float,price_list[-10:]))/10
                data.avp10_chg = data.avp10 - sum(map(float,price_list[-11:-1]))/10
                data.avp5 = sum(map(float,price_list[-5:]))/5
                data.avp5_chg = data.avp5 - sum(map(float,price_list[-6:-1]))/5
                data.avp3 = sum(map(float,price_list[-3:]))/3
                data.avp3_chg = data.avp3 - sum(map(float,price_list[-4:-1]))/3
                data.avp20_chg5 = data.avp20 - sum(map(float,price_list[-25:-5]))/20
                data.avp60_chg5 = data.avp60 - sum(map(float,price_list[-65:-5]))/60

                if i > 0:
                    data.preclose = stock_info_list[i-1].close
                    data.pre_pct_chg = stock_info_list[i-1].pct_chg
                    data.pct_chg = (data.close - data.preclose) / data.preclose * 100
                    data.amplitude = (data.high - data.low) / data.preclose * 100

                stock_info_list[i] = data
                db.session.add(data)
                db.session.add(data)
                db.session.commit()
                # print("成功入库", data.name,data.date)

            print ("成功入库", stock["name"])

    """
    从 baostoc导入所有股票数据
    """
    def save_all_stocks(self):
        # # pool = ThreadPoolExecutor(10)
        # rs = self.baostock.query_stock_industry()
        # while (rs.error_code == '0') & rs.next():
        #     data = rs.get_row_data()
        #     stock = {
        #         "market":data[1][0:2],
        #         "code":data[1][3:],
        #         "name":data[2],
        #         "industry":data[3],
        #     }
        #     try:
        #         stock_basic = self.baostock.query_stock_basic(code=data[1]).get_row_data()
        #         if int(stock_basic[5])  == 0:
        #             print("退市", stock["name"])
        #             continue
        #     except Exception as e:
        #         pass
        #     self.save_stock_info(stock)
        #     # pool.submit(self.save_stock_info, stock)
        # # pool.shutdown()

        # with ProcessPoolExecutor(10) as pool:
            bao_data = self.baostock.query_stock_industry().get_data()#查询所有股票
            stock_list = bao_data.iterrows()
            for i, v in stock_list:
                stock = {
                    "market":v[1][0:2],
                    "code":v[1][3:],
                    "name":v[2],
                    "industry":v[3],
                }
                try:
                    stock_basic = self.baostock.query_stock_basic(code=v[1]).get_row_data()#查询股票基本信息
                    if int(stock_basic[5])  == 0:
                        print("退市", stock["name"])
                        continue
                except Exception as e:
                    pass
                self.save_stock_info(stock)
                # pool.submit(self.save_stock_info, stock)

    def get_stock_basic(self):
        basic = self.baostock.query_stock_basic("sz.512880").get_row_data()
        print(basic)

if __name__ == '__main__':

    BaoStockImport().save_all_stocks()






