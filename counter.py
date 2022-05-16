import baostock as bs
import pandas as pd
import math,datetime,requests,sys,re,json,time,random,pandas,hashlib, sklearn, numpy

import pyttsx3
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

class Counter(object):

    said = []
    tts = None

    stock_group = {
        "s4gd":[250000, "002389000729002149000977605099000768002100002727003031"],
        # "gd":[150000, "603655001208001207603171603324002988603876605011002013600655002862"],
        "dzy":[50000, "002191603429601515603020300134"],
        "yqs":[100000, "000998600009001207003035600095603529002152601377603098"],
        "zndw":[50000, "000967002298000021002617000682600468002498"],
        # "cd":[100000, "600105600416600973002498002130002149"],
        # "djg":[100000, "300118300080601137002011002514600131"],
        "dxf":[100000, "600132603605603369603198002507600305603719"],
        "qn":[50000, "002549000723000338002249600860000572"],
        "hd":[50000, "002015603488002334002276002026600733"],
        "fd":[50000, "600875603985603063603606601218000875"],
        "nbq":[50000, "002660002518002965600405002364"],
        "ssf":[50000, "605337601995002368000906002824603039603520"],
    }

    def __init__(self):
        self.tts = pyttsx3.init()
        self.tts.setProperty('voice', 'zh')

    def  __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def say(self, say, say_key):
        print(say)
        # self.tts.say(say)
        # self.tts.runAndWait()
        self.said.append(say_key)

    def stock_real_time(self,  stock: str = "600094"):
        """
        东方财富网-数据中心-资金流向-个股
        http://data.eastmoney.com/zjlx/detail.html
        :param stock: 股票代码
        :type stock: str
        :param market: 股票市场; 上海证券交易所: sh, 深证证券交易所: sz
        :type market: str
        :return: 实时涨跌幅及主力资金数据
        :rtype: dict
        """
        market = "sh" if stock.startswith('60') or stock.startswith('900') else "sz"
        market_map = {"sh": 1, "sz": 0}
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        }
        params = {
            "fltt": "2",
            "invt": "2",
            "klt": "1",
            "secid": f"{market_map[market]}.{stock}",
            "fields": "f43,f44,f45,f46,f47,f48,f50,f57,f58,f60,f107,f137,f162,f168,f169,f170,f171,f177,f193",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "cb": "jQuery183003743205523325188_1589197499471",
            "_": int(time.time() * 1000),
        }
        r = requests.get(url, params=params, headers=headers)
        text_data = r.text
        json_data = json.loads(text_data[text_data.find("{") : -2])
        data = json_data["data"]
        return {
            "代码":data["f57"],
            "名称":data["f58"],
            "最高":data["f44"] if data["f44"] != "-" else data["f60"],
            "最低":data["f45"] if data["f45"] != "-" else data["f60"],
            "最新价":data["f43"] if data["f43"] != "-" else data["f60"],
            "开盘":data["f46"] if data["f46"] != "-" else data["f60"],
            "昨收":to_decimal(data["f60"]),
            "涨跌幅":to_decimal(data["f170"]),
            "主力净额":to_decimal(data["f137"]),
            "主力净比":to_decimal(data["f193"]),
            "量比":to_decimal(data["f50"]),
            "换手率":to_decimal(data["f168"]),
            "市盈率":to_decimal(data["f162"]),
        }

    def equity_structure (self, stock: str = "600004") -> pd.DataFrame:
        """
        新浪财经-财务报表-财务摘要
        https://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinanceSummary/stockid/600004.phtml
        :param stock: 股票代码
        :type stock: str
        :return: 新浪财经-财务报表-财务摘要
        :rtype: pandas.DataFrame
        """
        url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/{stock}.phtml"
        # url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinanceSummary/stockid/{stock}.phtml"
        r = requests.get(url)
        temp_df = pd.read_html(r.text)[12].iloc[:, :2]
        big_df = pd.DataFrame()
        for i in range(0, len(temp_df), 12):
            truncated_df = temp_df.iloc[i: i + 11, 1]
            big_df = pd.concat([big_df, truncated_df.reset_index(drop=True)], axis=1, ignore_index=True)
        data_df = big_df.T
        data_df.columns = temp_df.iloc[:11, 0].tolist()
        return data_df

    def get_group_stock_list(self, group_code):
        with DataBase() as db:
            stock_list = db.session.query(StockGroup).filter(*[
                StockGroup.group_code == group_code,
                ]).order_by(StockGroup.id.asc()).all()
            if len(stock_list) > 0:
                for stock in stock_list:
                    data = ak.stock_zh_index_daily_em(ak.stock_a_code_to_symbol(stock.code), beg = (datetime.datetime.now() + datetime.timedelta(days=-5)).strftime("%Y%m%d"))
                    last_kline = data.iloc[-1].to_dict()
                    last_kline["percent"] = stock.percent
                    stock_list[stock_list.index(stock)] = last_kline
                return stock_list
            return []

    def add_group_stock_list(self, group_code):
        """
        计算一个股票组合的各成员权重占比(按流通市值加权)
        :param stock_code_list:
        :return:
        """
        with DataBase() as db:
            stock_list = self.get_group_stock_list(group_code)
            if stock_list:
                return stock_list
            stock_code_list = [self.stock_group[group_code][1][n: n + 6] for n in range(0, len(self.stock_group[group_code][1]), 6)]
            for code in stock_code_list:
                # print(code)
                data = ak.stock_zh_index_daily_em(ak.stock_a_code_to_symbol(code), beg = "20211126")
                last_kline = data.iloc[-1].to_dict()
                shares = decimal.Decimal(last_kline["volume"])*100/(decimal.Decimal(last_kline["turn"])/100)
                price = decimal.Decimal(data.iloc[-1]["close"])
                market_value  =  shares * price
                inc = 100000000
                for n in range(1, int(market_value/inc)):
                    inc = inc * 2
                    if inc > market_value:
                        inc = n
                data.iloc[-1]["turn"] = data.iloc[-1]["turn"]*inc
                avp_turn = decimal.Decimal(sum([v["turn"] for v in list(data.iloc)[-5:]]))/5/5
                avp_turn = max(1, avp_turn)
                market_value =  market_value / avp_turn
                last_kline["market_value"] = market_value
                last_kline["close"] = decimal.Decimal(last_kline["close"])
                stock_list.append(last_kline)
            sorted_stock_list = sorted(stock_list, key = lambda v: v["market_value"])
            for stock in sorted_stock_list:
                total_value = sum([v["market_value"] for v in stock_list])
                share = stock["market_value"]/total_value
                if share > 0.2:
                    stock_list[stock_list.index(stock)]["market_value"] = stock["market_value"] / (share/decimal.Decimal(0.2))
                elif share < 0.05:
                    stock_list[stock_list.index(stock)]["market_value"] = stock["market_value"] * (decimal.Decimal(0.05)/share)
            total_value = sum([v["market_value"] for v in stock_list])
            for stock in stock_list:
                data = StockGroup(
                    group_code = group_code,
                    code = stock["code"],
                    name = stock["name"],
                    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    percent = stock["market_value"]/total_value
                )
                db.session.add(data)
                db.session.commit()
                stock_list[stock_list.index(stock)]["percent"] = data.percent

        return stock_list

if __name__ == '__main__':
    core = Counter()
    for group_code in core.stock_group:
        stock_list = core.add_group_stock_list(group_code)
        print("==================================================")
        print(group_code, round(sum([ v["percent"]*decimal.Decimal(v["pct_chg"]) for v in stock_list]), 4))
        for stock_i,  stock in enumerate(stock_list):
            hold = round(stock["percent"]*decimal.Decimal(core.stock_group[group_code][0])/decimal.Decimal(stock["close"])/decimal.Decimal(100))*decimal.Decimal(100)
            # print(stock_i + 1, stock["code"], stock["name"], round(stock["percent"]*100, 2), hold, round(decimal.Decimal(stock["close"])*hold, 2), round(stock["close"], 2))
            symb = "1," if stock["code"].startswith('60') or stock["code"].startswith('900') else "0,"
            print(symb + stock["code"] + "," + str(round(stock["percent"]*100, 2)))

        for stock_i,  stock in enumerate(stock_list):
            hold = round(stock["percent"]*decimal.Decimal(core.stock_group[group_code][0])/decimal.Decimal(stock["close"])/decimal.Decimal(100))*decimal.Decimal(100)
            print(stock_i + 1, stock["code"], stock["name"], round(stock["percent"]*100, 2), hold, round(decimal.Decimal(stock["close"])*hold, 2), round(stock["close"], 2))
            symb = "1," if stock["code"].startswith('60') or stock["code"].startswith('900') else "0,"
            # print(symb + stock["code"] + "," + str(round(stock["percent"]*100, 2)))
