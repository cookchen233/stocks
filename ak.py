import baostock as bs
import pandas as pd
import math,datetime,requests,re,json,time,random,pandas,hashlib, sklearn, numpy
from bs4 import BeautifulSoup
from matplotlib import ticker
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
import numpy as np
import matplotlib.pyplot as plt
import time

from tool import *
from model.connecter import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed
import easyquotation

import akshare as ak

class AKShareImport(object):

    def __init__(self):
        ak.stock_zh_a_hist = retry_decorator(ak.stock_zh_a_hist)
        self.em_stock_daily = retry_decorator(self.em_stock_daily)

    def  __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def em_stock_daily(self, symbol: str = "000913", beg="20050101", end="20250101" ) -> pd.DataFrame:
        """
        东方财富股票指数数据
        http://quote.eastmoney.com/center/hszs.html
        :param symbol: 带市场标识的指数代码
        :type symbol: str
        :return: 指数数据
        :rtype: pandas.DataFrame
        """

        if "." in symbol:
            pass
        elif "sh" in symbol:
            symbol = "1." + symbol[2:]
        elif "sz" in symbol:
            symbol = "0." + symbol[2:]
        elif symbol.startswith("6") or symbol.startswith("900"):
            symbol = "1." + symbol
        else:
            symbol = "0." + symbol
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "cb": "jQuery1124033485574041163946_1596700547000",
            "secid": symbol,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f61",#f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65,f66,f67
            "klt": "101",  # 日频率
            "fqt": "0",
            "beg": beg,
            "end": end,
            "_": "1596700547039",
        }
        r = requests.get(url, params=params)
        data_text = r.text
        data_json = json.loads(data_text[data_text.find("{"):-2])
        temp_df = pd.DataFrame([(item + "," + data_json["data"]["code"] + "," + data_json["data"]["name"]).split(",") for item in data_json["data"]["klines"]])
        if len(data_json["data"]["klines"]) < 1:
            return temp_df
        temp_df.columns = ["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "pct_chg", "turn", "code", "name", ]
        temp_df = temp_df[["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "pct_chg", "turn", "code", "name", ]]
        temp_df = temp_df.astype({
            "open": float,
            "close": float,
            "high": float,
            "low": float,
            "volume": float,
            "amount": float,
            "amplitude": float,
            "pct_chg": float,
            "turn": float,
        })
        return temp_df

    def get_etf_members(self, code) -> list:
        url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&topline=30&code=" + code
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "lxml")
        try:
            list = soup.select_one("div.box").select("tbody > tr")
        except Exception as e:
            print(e, code)
            return []
        members=[]
        for li in list:
            code = li.find_all("td")[1].find("a").text
            members.append(code)
        return members

    def get_index_members(self, code):
        members = []
        try:
            ak_data = ak.index_stock_cons(code)
        except AttributeError as e:
            print(e, code)
            return []
        for i, v in ak_data.iterrows():
            members.append(v["品种代码"])
            if i > 30:
                break
        return members



    def save_etf_daily(self, stock):
        try:
            with DataBase() as db:
                price_list = []
                volume_list = []
                etf_members = self.get_etf_members(stock["code"][2:])
                members = ",".join(etf_members)
                top_members = ",".join(sorted(etf_members[0:4]))#前4重仓股按代码排序, 以此判断是否为相似 ETF

                # ak_data = ak.stock_zh_a_hist(stock["代码"], start_date = "20050101", adjust="")#查询股票日 k 线数据
                # 查询股票日k线数据
                ak_data = self.em_stock_daily(stock["code"], beg="20050101")
                daily_list = list(ak_data.iterrows())
                for i, v in daily_list:
                    price_list.append(to_decimal(v["close"]))
                    volume_list.append(to_decimal(max(v["volume"], 1)))

                    db_data = db.session.query(EtfDaily).filter(*[
                        EtfDaily.date == v["date"],
                        EtfDaily.code == stock["code"][2:],
                        ]).first()
                    if db_data:
                        daily_list[i] = db_data
                        continue

                    data = EtfDaily(
                        code = stock["code"][2:],
                        name = stock["name"],
                        # industry = stock["名称"],
                        date = v["date"],
                        open = to_decimal(v["open"]),
                        high = to_decimal(v["high"]),
                        low = to_decimal(v["low"]),
                        close = to_decimal(v["close"]),
                        pct_chg = to_decimal(v["pct_chg"]),
                        amplitude = to_decimal(v["amplitude"]),
                        volume = to_decimal(max(v["volume"], 1)),
                        amount = to_decimal(max(v["amount"], 1)),
                        turnover = to_decimal(max(v["turn"], 0.0001)),
                        top_members = top_members,
                        members = members,
                    )

                    if i > 0:
                        data.pre_close = daily_list[i-1].close
                        data.pre_pct_chg = daily_list[i-1].pct_chg
                        data.pre_turnover = daily_list[i-1].turnover
                    if i > 5:
                        data.volume_ratio = to_decimal(data.volume/sum(volume_list[-6:-1])/5).quantize(decimal.Decimal('0.00'))
                    if len(price_list) > 121:
                        data.avp5 = sum(map(float,price_list[-5:]))/5
                        data.avp20 = sum(map(float,price_list[-20:]))/20
                        data.avp30 = sum(map(float,price_list[-30:]))/30
                        data.avp60 = sum(map(float,price_list[-60:]))/60
                        data.avp120 = sum(map(float,price_list[-120:]))/120

                        data.avp20_chg5 = data.avp20 - sum(map(float,price_list[-25:-5]))/20
                        data.avp60_chg5 = data.avp60 - sum(map(float,price_list[-65:-5]))/60

                    data.free_shares = data.volume*to_decimal(100)/(data.turnover/to_decimal(100))
                    data.market_value  =  int(data.free_shares * data.close)

                    daily_list[i] = data
                    db.session.add(data)
                    db.session.commit()
                    # print("成功入库", data.name,data.date)

                print ("成功入库", stock["name"], stock["序号"])
        except Exception as e:
            err_log(stock["code"], stock["name"])

    def save_all_etfs(self):
        with DataBase() as db:
            sh_index = db.session.query(IndexDaily).filter(*[
                StockDaily.code == "000001",
                ]).order_by(IndexDaily.date.desc()).first()
        ak_data = ak.fund_etf_category_sina("ETF基金")#查询所有 ETF
        pool = ThreadPoolExecutor(20)
        for i, v in ak_data.iterrows():
            if decimal.Decimal(v["成交额"]) < 10000000:
                continue
            with DataBase() as db:
                if db.session.query(StockDaily).filter(*[
                    StockDaily.date == sh_index.date,
                    StockDaily.code == v["代码"],
                ]).first():
                    continue
            etf = {
                "code":v["代码"],
                "name":v["名称"],
                "序号":"{}/{}".format(i, ak_data.index.size),
            }
            self.save_etf_daily(etf)
            # pool.submit(self.save_etf_daily, etf)
        pool.shutdown()

    def save_index_daily(self, stock):
        try:
            with DataBase() as db:
                price_list = []
                volume_list = []
                index_members = self.get_index_members(stock["code"])
                members = ",".join(index_members)
                top_members = ",".join(sorted(index_members[0:4]))#前4重仓股按代码排序, 以此判断是否为相似 ETF

                # ak_data = ak.stock_zh_a_hist(stock["代码"], start_date = "20050101", adjust="")#查询股票日 k 线数据
                # 查询股票日k线数据
                ak_data = self.em_stock_daily("1." + stock["code"], beg="19900101")
                # print(ak_data)
                # return
                daily_list = list(ak_data.iterrows())
                for i, v in daily_list:
                    price_list.append(to_decimal(v["close"]))
                    volume_list.append(to_decimal(max(v["volume"], 1)))

                    db_data = db.session.query(IndexDaily).filter(*[
                        IndexDaily.date == v["date"],
                        IndexDaily.code == stock["code"],
                        ]).first()
                    if db_data:
                        daily_list[i] = db_data
                        continue

                    data = IndexDaily(
                        code = stock["code"],
                        name = stock["name"],
                        # industry = stock["name"],
                        date = v["date"],
                        open = to_decimal(v["open"]),
                        high = to_decimal(v["high"]),
                        low = to_decimal(v["low"]),
                        close = to_decimal(v["close"]),
                        pct_chg = to_decimal(v["pct_chg"]),
                        amplitude = to_decimal(v["amplitude"]),
                        volume = to_decimal(max(v["volume"], 1)),
                        amount = to_decimal(max(v["amount"], 1)),
                        turnover = to_decimal(max(v["turn"], 0.0001)),
                        top_members = top_members,
                        members = members,
                    )

                    if i > 0:
                        data.pre_close = daily_list[i-1].close
                        data.pre_pct_chg = daily_list[i-1].pct_chg
                        data.pre_turnover = daily_list[i-1].turnover
                    if i > 5:
                        data.volume_ratio = to_decimal(data.volume/sum(volume_list[-6:-1])/5).quantize(decimal.Decimal('0.00'))
                    if len(price_list) > 121:
                        data.avp5 = sum(map(float,price_list[-5:]))/5
                        data.avp20 = sum(map(float,price_list[-20:]))/20
                        data.avp30 = sum(map(float,price_list[-30:]))/30
                        data.avp60 = sum(map(float,price_list[-60:]))/60
                        data.avp120 = sum(map(float,price_list[-120:]))/120

                        data.avp20_chg5 = data.avp20 - sum(map(float,price_list[-25:-5]))/20
                        data.avp60_chg5 = data.avp60 - sum(map(float,price_list[-65:-5]))/60

                    data.free_shares = data.volume*to_decimal(100)/(data.turnover/to_decimal(100))
                    data.market_value  =  int(data.free_shares * data.close)

                    daily_list[i] = data
                    db.session.add(data)
                    db.session.commit()
                    # print("成功入库", data.name,data.date)

                print ("成功入库", stock["name"], stock["序号"])
        except Exception as e:
            err_log(stock["code"], stock["name"])

    def save_all_indices(self):
        ak_data = ak.index_stock_info()#查询所有指数
        pool = ThreadPoolExecutor(20)
        for i, v in ak_data.iterrows():
            stock = {
                "code":v["index_code"],
                "name":v["display_name"],
                "序号":i,
            }
            self.save_index_daily(stock)
        pool.shutdown()

    def save_stock_daily(self, stock):
        try:
            with DataBase() as db:
                price_list = []
                volume_list = []
                # ak_data = ak.stock_zh_a_hist(stock["代码"], start_date = "20050101", adjust="")#查询股票日 k 线数据
                # 查询股票日k线数据
                ak_data = self.em_stock_daily(stock["代码"], beg="20050101")
                daily_list = list(ak_data.iterrows())
                for i, v in daily_list:
                    price_list.append(to_decimal(v["close"]))
                    volume_list.append(to_decimal(max(v["volume"], 1)))

                    db_data = db.session.query(StockDaily).filter(*[
                        StockDaily.date == v["date"],
                        StockDaily.code == stock["代码"],
                        ]).first()
                    if db_data:
                        daily_list[i] = db_data
                        continue

                    data = StockDaily(
                        code = stock["代码"],
                        name = stock["名称"],
                        # industry = stock["名称"],
                        date = v["date"],
                        open = to_decimal(v["open"]),
                        high = to_decimal(v["high"]),
                        low = to_decimal(v["low"]),
                        close = to_decimal(v["close"]),
                        pct_chg = to_decimal(v["pct_chg"]),
                        amplitude = to_decimal(v["amplitude"]),
                        volume = to_decimal(max(v["volume"], 1)),
                        amount = to_decimal(max(v["amount"], 1)),
                        turnover = to_decimal(max(v["turn"], 0.0001)),
                    )

                    if i > 0:
                        data.pre_close = daily_list[i-1].close
                        data.pre_pct_chg = daily_list[i-1].pct_chg
                        data.pre_turnover = daily_list[i-1].turnover
                        if (data.pre_close * to_decimal(1.1)).quantize(decimal.Decimal('0.00'), rounding=decimal.ROUND_HALF_UP) == data.close:
                            data.is_limit_up = 1
                            data.keep_limit_up_days = daily_list[i-1].keep_limit_up_days + 1
                    if i > 5:
                        data.volume_ratio = to_decimal(data.volume/sum(volume_list[-6:-1])/5).quantize(decimal.Decimal('0.00'))
                    if len(price_list) > 121:
                        data.avp5 = sum(map(float,price_list[-5:]))/5
                        data.avp20 = sum(map(float,price_list[-20:]))/20
                        data.avp30 = sum(map(float,price_list[-30:]))/30
                        data.avp60 = sum(map(float,price_list[-60:]))/60
                        data.avp120 = sum(map(float,price_list[-120:]))/120

                        data.avp20_chg5 = data.avp20 - sum(map(float,price_list[-25:-5]))/20
                        data.avp60_chg5 = data.avp60 - sum(map(float,price_list[-65:-5]))/60

                    data.free_shares = data.volume*to_decimal(100)/(data.turnover/to_decimal(100))
                    data.market_value  =  int(data.free_shares * data.close)

                    daily_list[i] = data
                    db.session.add(data)
                    db.session.commit()
                    # print("成功入库", data.name,data.date)

                print ("成功入库", stock["名称"], stock["序号"])
        except Exception as e:
            err_log(stock["代码"], stock["名称"])

    def save_all_stock_daily(self):
        with DataBase() as db:
            sh_index = db.session.query(IndexDaily).filter(*[
                StockDaily.code == "000001",
            ]).order_by(IndexDaily.date.desc()).first()
        ak_data = ak.stock_zh_a_spot_em()#查询所有股票
        pool = ThreadPoolExecutor(10)
        for i, v in ak_data.iterrows():
            if v["名称"][-1] == "退" or v["名称"][:2] == "退市":
                continue
            with DataBase() as db:
                if db.session.query(StockDaily).filter(*[
                    StockDaily.date == sh_index.date,
                    StockDaily.code == v["代码"],
                ]).first():
                    continue
            # info = ak.stock_individual_info_em("688138")
            v["序号"] = "{}/{}".format(i, ak_data.index.size)
            # pool.submit(self.save_stock_daily, v)
            self.save_stock_daily(v)
        pool.shutdown()

if __name__ == '__main__':
    impt = AKShareImport()
    # while True:
    #     impt.save_all_etfs()
    #     time.sleep(86400)

    while True:
        stock = {
            "code":"000001",
            "name":"上证指数",
            "序号":1,
        }
        impt.save_index_daily(stock)
        # impt.save_all_stock_daily()
        # impt.save_all_etfs()
        time.sleep(86400)

    # print(impt.em_stock_daily("688555"))
    # print(ak.stock_zh_index_daily_em(ak.stock_a_code_to_symbol("688555")))


    # ak_data = ak.index_stock_info()#查询所有指数
    # ak_data = ak.index_stock_cons("881152")#查询指数成份股
    # print(ak_data.to_csv("xx.csv"))

    # ak_data = ak.fund_etf_category_sina("ETF基金").iterrows()
    # for i in ak_data:
    #     print(i)
    #     break

    # stock = {
    #     "code":"000001",
    #     "name":"上证指数",
    #     "序号":1,
    # }
    # impt.save_index_daily(stock)
    # stock = {
    #     "code":"90.BK0816",
    #     "name":"昨日连板",
    #     "序号":2,
    # }
    # impt.save_index_daily(stock)