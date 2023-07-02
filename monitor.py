#!/usr/local/bin/python3

import argparse
import queue
import threading
from subprocess import call

import baostock as bs
import pandas as pd
import math,datetime,requests,re,json,time,random,pandas,hashlib, sklearn, numpy

import pyttsx3
from bs4 import BeautifulSoup
from matplotlib import ticker
from pyttsx3 import Engine
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
from PIL import ImageGrab

import akshare as ak
import counter


class Monitor(object):

    record = {}
    tts = None
    session = None
    notify_queues = None


    def __init__(self):
        self.notify_queues = {
            "say": {"consumer": self.__say_consumer, "queue": queue.Queue()},
            "dingding": {"consumer": self.__dingding_consumer, "queue": queue.Queue()},
            "log": {"consumer": self.__log_consumer, "queue": queue.Queue()},
        }
        for qk, q in self.notify_queues.items():
            t = threading.Thread(target=q["consumer"], args=(q["queue"],))
            t.daemon = True
            t.start()

        self.session = requests.Session()
        self.__get_live_data = retry_decorator(self.__get_live_data)
        ak.stock_zh_index_daily_em = retry_decorator(ak.stock_zh_index_daily_em)

    def  __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __say_consumer(self, queue):
        while True:
            msg = queue.get()
            msg = ", ".join(msg)
            call(["python3", os.path.abspath(os.path.dirname(__file__)) + "/speak.py", msg])

    def __log_consumer(self, queue):
        while True:
            msg = queue.get()
            log("monitor", *msg)

    def __dingding_consumer(self, queue):
        while True:
            msg = queue.get()
            msg = "完成率: " + str(msg)
            token="https://oapi.dingtalk.com/robot/send?="
            headers={'Content-Type':'application/json'}
            data={"msgtype":"text","text":{ "content": msg}}
            requests.post(token,data=json.dumps(data),headers=headers)

    def dingding(self, msg):
        self.notify_queues["dingding"]["queue"].put(msg)

    def log(self, *msg):
        self.notify_queues["log"]["queue"].put(msg)

    def say(self, *msg):
        self.notify_queues["say"]["queue"].put(msg)

    def is_recorded(self, record_key, seconds):
        if record_key in self.record:
            return (datetime.datetime.now() - self.record[record_key]).total_seconds() < seconds
        return False

    def set_record_time(self, record_key):
        self.record[record_key] = datetime.datetime.now()

    def __stock_real_time(self,  stock: str = "600094", market: str = "sh"):
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
        pr(r.url)
        text_data = r.text
        json_data = json.loads(text_data[text_data.find("{") : -2])
        data = json_data["data"]
        return {
            "代码":data["f57"],
            "名称":data["f58"],
            "最高":to_decimal(data["f44"]) if data["f44"] != "-" else to_decimal(data["f60"]),
            "最低":to_decimal(data["f45"]) if data["f45"] != "-" else to_decimal(data["f60"]),
            "最新价":to_decimal(data["f43"]) if data["f43"] != "-" else to_decimal(data["f60"]),
            "开盘":to_decimal(data["f46"]) if data["f46"] != "-" else to_decimal(data["f60"]),
            "昨收":to_decimal(data["f60"]),
            "涨跌幅":to_decimal(data["f170"]),
            "主力净额":to_decimal(data["f137"]),
            "主力净比":to_decimal(data["f193"]),
            "量比":to_decimal(data["f50"]),
            "换手":to_decimal(data["f168"]),
            "市盈率":to_decimal(data["f162"]),
        }

    def __get_live_data(self, code):
        code = f"1.{code}" if code.startswith('60') or code.startswith('900') or code.startswith('11') or code.startswith('688') else f"0.{code}"
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        }
        params = {
            "fltt": "2",
            "invt": "2",
            "klt": "1",
            "secid": code,
            "fields": "f57,f58,f60,f43,f44,f45,f47,f170,f19,f20,f39,f40,f530",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            # "cb": "jQuery183003743205523325188_1589197499471",
            "_": int(time.time() * 1000),
        }
        r = self.session.get(url, params=params, headers=headers)
        data = json.loads(r.text)["data"]
        # if not data:
        #     print(code)
        return {
            "code": data["f57"],
            "name": data["f58"],
            "pre_close": float(data["f60"]),
            "close": float(data["f43"]) if data["f43"] != "-" else to_decimal(data["f60"]),
            "high": float(data["f44"]) if data["f44"] != "-" else to_decimal(data["f60"]),
            "low": float(data["f45"]) if data["f45"] != "-" else to_decimal(data["f60"]),
            "pct_chg": float(data["f170"]) if data["f170"] != "-" else 0,
            "buy1_lots": int(data["f20"]) if data["f20"] != "-" else 0,
            "sell1_lots": int(data["f40"]) if data["f40"] != "-" else 0,
            "volume": int(data["f47"]) if data["f47"] != "-" else 0,
        }

    def __symbol_code(self, code):
        return f"sh{code}" if code.startswith('60') or code.startswith('900') or code.startswith('11') or code.startswith('5') else f"sz{code}"

    def scan_etf(self):
        ak_data = ak.fund_etf_category_sina("ETF基金")#查询所有 ETF
        pick_list = []
        for i, v in ak_data.iterrows():
            if decimal.Decimal(v["成交额"]) < 10000000:
                continue
            code = v["代码"][2:]
            stock = self.__get_stock_recent_price(code)
            c = code[::-1]
            notify_code = "0." + c[0:3] + c[3:]
            msg = ""
            is_notify = False
            if stock["涨跌幅"] < -4:
                msg = "暴跌" + str(stock["涨跌幅"])
                # is_notify = True
            elif stock["两日连跌"] < -5:
                msg = "两日连跌" + str(stock["两日连跌"])
            elif stock["三日连跌"] < -4:
                msg = "三日连跌" + str(stock["三日连跌"])
            elif stock["四日连跌"] < -5:
                msg = "四日连跌" + str(stock["四日连跌"])
            elif stock["5日涨跌"] < -5:
                msg = "5日涨跌" + str(stock["5日涨跌"])
            if msg != "":
                if is_notify:
                    record_key = notify_code + ", low"
                    if not self.is_recorded(record_key, 1800):
                        self.dingding(record_key)
                        self.log(stock["名称"] + msg)
                        self.say(stock["名称"] + msg)
                        self.set_record_time(record_key)
                pick_list.append({"代码":stock["代码"], "名称":stock["名称"], "涨跌幅":stock["涨跌幅"], "两日连跌": stock["两日连跌"], "三日连跌":stock["三日连跌"], "5日涨跌":stock["5日涨跌"], "10日涨跌":stock["10日涨跌"], "记录依据": msg})
        print("标的数", len(pick_list))
        if len(pick_list) > 0:
            pk = pick_list
            filename = "/Users/chen/maat/coding/python/stocks/data/有前途的etf-" + str(datetime.datetime.now().date()) + ".xlsx"
            if os.path.exists(filename):
                ex_data = pd.read_excel(filename).iterrows()
                for v in ex_data:
                    is_existed = False
                    for v1 in pick_list:
                        if int(v1["代码"]) == int(v[1]["代码"]):
                            is_existed = True
                            break
                    if not is_existed:
                        pk.append({"代码":str(v[1]["代码"]), "名称":v[1]["名称"], "涨跌幅":v[1]["涨跌幅"], "两日连跌": v[1]["两日连跌"], "三日连跌":v[1]["三日连跌"], "5日涨跌":v[1]["5日涨跌"], "10日涨跌":v[1]["10日涨跌"], "记录依据": v[1]["记录依据"]})

            pd.DataFrame(pk).sort_values(by=['10日涨跌']).to_excel(filename)
            # pd.DataFrame(pk).sort_values(by=['10日涨跌']).to_csv("/Users/chen/maat/coding/python/stocks/data/" + str(datetime.datetime.now().date()) + "_pick_etf.txt",sep='\t',index=False)

    def scan_buying2(self):
        def __scan_buying_data(data):
            pick_list = []
            for i, v in data:
                code = v["代码"][2:]
                stock = self.__get_stock_recent_price(code)
                c = code[::-1]
                notify_code = "0." + c[0:3] + c[3:]
                msg = ""
                if stock["涨跌幅"] < -7:
                    msg = "暴跌" + str(stock["涨跌幅"])
                elif stock["两日连跌"] < -10:
                    msg = "两日连跌" + str(stock["两日连跌"])
                elif stock["三日连跌"] < -8:
                    msg = "三日连跌" + str(stock["三日连跌"])
                elif stock["四日连跌"] < -10:
                    msg = "四日连跌" + str(stock["四日连跌"])
                elif stock["5日涨跌"] < -10:
                    msg = "5日涨跌" + str(stock["5日涨跌"])
                if msg != "":
                    record_key = notify_code + ", low"
                    if not self.is_recorded(record_key, 3600):
                        self.dingding(record_key)
                        self.log(stock["名称"] + msg)
                        self.say(stock["名称"] + msg)
                        self.set_record_time(record_key)

                    pick_list.append({"代码":stock["代码"], "名称":stock["名称"], "涨跌幅":stock["涨跌幅"], "两日连跌": stock["两日连跌"], "三日连跌":stock["三日连跌"], "5日涨跌":stock["5日涨跌"], "10日涨跌":stock["10日涨跌"], "记录依据": msg})

            if len(pick_list) > 0:
                pk = pick_list
                filename = "/Users/chen/maat/coding/python/stocks/data/猥琐发育不要浪-" + str(datetime.datetime.now().date()) + ".xlsx"
                if os.path.exists(filename):
                    ex_data = pd.read_excel(filename).iterrows()
                    for v in ex_data:
                        e = 0
                        for v1 in pick_list:
                            if int(v1["代码"]) == int(v[1]["代码"]):
                                e = 1
                                break
                        if not e:
                            pk.append({"代码":str(v[1]["代码"]), "名称":v[1]["名称"], "涨跌幅":v[1]["涨跌幅"], "两日连跌": v[1]["两日连跌"], "三日连跌":v[1]["三日连跌"], "5日涨跌":v[1]["5日涨跌"], "10日涨跌":v[1]["10日涨跌"], "记录依据": v[1]["记录依据"]})

                pd.DataFrame(pk).sort_values(by=['10日涨跌']).to_excel(filename)
                # pd.DataFrame(pk).sort_values(by=['10日涨跌']).to_csv("/Users/chen/maat/coding/python/stocks/data/" + str(datetime.datetime.now().date()) + "_pick_etf.txt",sep='\t',index=False)


        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        data = pd.read_excel("/Users/chen/Downloads/猥琐发育.xlsx", usecols=[0], skiprows=[0], names = ['代码']).iterrows()
        __scan_buying_data(data)

    def scan_buying(self):
        def __scan_buying_data(code):
            stock = self.__get_stock_recent_price(code)
            c = code[::-1]
            notify_code = "0." + c[0:3] + c[3:]
            msg = ""
            if stock["涨跌幅"] < -7:
                msg = "暴跌" + str(stock["涨跌幅"])
            elif stock["涨跌幅"] > 2:
                msg = "暴涨" + str(stock["涨跌幅"])
            elif (stock["压力"] - stock["现价"])/stock["现价"]*100 <= 1:
                msg = "触及压力位"
            elif (stock["支撑"] - stock["现价"])/stock["现价"]*100 >= -1:
                msg = "触及支撑位"
            elif stock["两日连跌"] < -15:
                msg = "两日连跌" + str(stock["两日连跌"])
            elif stock["三日连跌"] < -20:
                msg = "三日连跌" + str(stock["三日连跌"])
            elif stock["四日连跌"] < -15:
                msg = "四日连跌" + str(stock["四日连跌"])
            elif stock["5日涨跌"] < -15:
                msg = "5日涨跌" + str(stock["5日涨跌"])
            elif stock["高点回撤"] < -15:
                msg = "高点回撤" + str(stock["高点回撤"])
            if msg != "":
                record_key = notify_code + ", low"
                if not self.is_recorded(record_key, 600):
                    self.dingding(record_key)
                    self.log(stock["名称"] + msg)
                    self.say(stock["名称"] + msg)
                    self.set_record_time(record_key)

        code_list = self.__get_code_list(os.path.abspath(os.path.dirname(__file__)) + "/conf/buying_code_list.txt")
        for code in code_list:
            __scan_buying_data(code)
            time.sleep(0.2)

    def __get_stock_recent_price(self, code):
        kline = ak.stock_zh_index_daily_em(self.__symbol_code(code), beg=(datetime.datetime.now() + datetime.timedelta(days=-15)).strftime("%Y%m%d"), fq="1")
        stock = {
            "代码":code,
            "名称":kline.iloc[-1]["name"],
            "涨跌幅": round(kline.iloc[-1]["pct_chg"], 2),
        }
        highest = 0
        lowest = 100
        for k in kline.iloc[:-1].to_dict("records"):
            if k["high"] > highest:
                highest = k["high"]
            if k["low"] < lowest:
                lowest = k["low"]
        stock["两日连跌"] = round(kline.iloc[-1]["pct_chg"] + kline.iloc[-2]["pct_chg"], 2) if kline.shape[0] > 1 and kline.iloc[-1]["pct_chg"] < 0 and kline.iloc[-2]["pct_chg"] < 0 else 0
        stock["三日连跌"] = round(kline.iloc[-1]["pct_chg"] + kline.iloc[-2]["pct_chg"] + kline.iloc[-3]["pct_chg"], 2) if kline.shape[0] > 2 and kline.iloc[-1]["pct_chg"] < 0 and kline.iloc[-2]["pct_chg"] < 0 and kline.iloc[-3]["pct_chg"] < 0 else 0
        stock["三日连跌"] = round(kline.iloc[-1]["pct_chg"] + kline.iloc[-2]["pct_chg"] + kline.iloc[-3]["pct_chg"], 2) if kline.shape[0] > 2 and kline.iloc[-1]["pct_chg"] < 0 and kline.iloc[-2]["pct_chg"] < 0 and kline.iloc[-3]["pct_chg"] < 0 else 0
        stock["四日连跌"] = round(kline.iloc[-1]["pct_chg"] + kline.iloc[-2]["pct_chg"] + kline.iloc[-3]["pct_chg"] + kline.iloc[-4]["pct_chg"], 2) if kline.shape[0] > 3 and kline.iloc[-1]["pct_chg"] < 0 and kline.iloc[-2]["pct_chg"] < 0 and kline.iloc[-3]["pct_chg"] < 0 and kline.iloc[-4]["pct_chg"] < 0 else 0
        stock["5日涨跌"] = round((kline.iloc[-1]["close"] - kline.iloc[-6]["close"])/kline.iloc[-6]["close"]*100, 2) if kline.shape[0] > 5 else 0
        stock["10日涨跌"] = round((kline.iloc[-1]["close"] - kline.iloc[-11]["close"])/kline.iloc[-11]["close"]*100, 2)  if kline.shape[0] > 10 else 0
        stock["高点回撤"] = round((kline.iloc[-1]["close"] - highest)/highest*100, 2)
        stock["压力"] = highest
        stock["支撑"] = lowest
        stock["现价"] = kline.iloc[-1]["close"]
        return stock

    def scan_my_group(self):
        core = counter.Counter()
        group_list = ["fd", "dxf", "hd", "qn", "nbq", "zndw"]
        for group_code in group_list:
            try:
                stock_list = core.get_group_stock_list(group_code)
            except requests.ConnectionError:
                continue
            pct_chg = round(sum([ v["percent"]*decimal.Decimal(v["pct_chg"]) for v in stock_list]), 4)
            if pct_chg > 3 or pct_chg < -3:
                record_time = self.get_record_time(group_code)
                if datetime.datetime.now() < datetime.datetime.strptime(str(datetime.datetime.now().date()) + " 10:10", "%Y-%m-%d %H:%M"):
                    if (datetime.datetime.now() - record_time).total_seconds() >= 60:
                        pass
                        #self.dingding("{} {}".format(group_code, pct_chg), group_code)
                else:
                    if (datetime.datetime.now() - record_time).total_seconds() >= 600:
                        pass
                        #self.dingding("{} {}".format(group_code, pct_chg), group_code)

    last_buy1_lots = {}
    last_sell1_lots = {}
    last_pct_chg = {}
    last_volume = {}
    volume_diff_list = {}
    def __scan_up_stock(self, code, down_pct_dff = -1, up_pct_diff = 1, min_volume_diff = 5000000, min_lots_diff = 2000, min_lots = 20000, up_pct = 8, down_pct = 0):
        try:
            if not code or len(code)<6 or not code[0].isdigit():
                print("x")
                return

            data = self.__get_live_data(code)
            name = data["name"][0:2]

            #print(data["name"])
            c = code[::-1]
            notify_code = "0." + c[0:3] + c[3:]

            #停牌
            if data["buy1_lots"] == 0 and data["sell1_lots"] == 0:
                return

            #if data["volume"]*100*data["close"] < 50000000:
            #    return

            if code not in self.last_volume:
                self.__set_last_data(code, data)

            limit_up_pct = 0.05 if "st" in name.lower() else (0.1 if code.startswith('60') or code.startswith('90') or code.startswith('00') else 0.2)

            #跌停
            if data["buy1_lots"] == 0 and data["close"] <= round(data["pre_close"]*(1 - limit_up_pct), 2):
                #压单极少
                record_key = notify_code + ", ready_up"
                if not self.is_recorded(record_key, 10) and data["sell1_lots"] < min_lots:
                    lots = int(data["sell1_lots"]/10000)*10000
                    #self.dingding(record_key)
                    self.log(name + f"仅剩{lots}")
                    self.say(name + f"仅剩{lots}")
                    self.set_record_time(record_key)

                #压单大幅减少
                record_key = notify_code + ", going_up"
                lots_diff = int((self.last_sell1_lots[code] - data["sell1_lots"])/1000)*1000
                if not self.is_recorded(record_key, 2) and ((data["sell1_lots"] < 200000 and lots_diff > min_lots_diff) or data["sell1_lots"] > 500000 and lots_diff > min_lots_diff*2):
                    #self.dingding(record_key)
                    lots = int(data["sell1_lots"]/10000)*10000
                    self.log(name + f"减{lots_diff}, 剩{lots}")
                    self.say(name + f"减{lots_diff}, 剩{lots}")
                    self.set_record_time(record_key)
            #涨停
            elif data["sell1_lots"] == 0 and data["close"] >= round(data["pre_close"]*(1 + limit_up_pct), 2):
                #封单极少
                record_key = notify_code + ", ready_down"
                if not self.is_recorded(record_key, 10) and data["buy1_lots"] < min_lots:
                    lots = int(data["buy1_lots"]/10000)*10000
                    #self.dingding(record_key)
                    self.log(name + f"仅剩{lots}")
                    self.say(name + f"仅剩{lots}")
                    self.set_record_time(record_key)

                #封单大幅减少
                record_key = notify_code + ", going_down"
                lots_diff = int((self.last_buy1_lots[code] - data["buy1_lots"])/1000)*1000
                if not self.is_recorded(record_key, 2) and (data["buy1_lots"] < 200000 and lots_diff > min_lots_diff):
                    #self.dingding(record_key)
                    lots = int(data["buy1_lots"]/10000)*10000
                    self.log(name + f"减{lots_diff}, 剩{lots}")
                    self.say(name + f"减{lots_diff}, 剩{lots}")
                    self.set_record_time(record_key)
            else:
                pct_chg_diff = round(data["pct_chg"] - self.last_pct_chg[code], 1)
                #快速拉升
                # if pct_chg_diff > up_pct_diff:
                #     record_key = notify_code + ", fast_up"
                #     if not self.is_recorded(record_key, 5):
                #         self.dingding(record_key)
                #         self.log(name + f"急拉{pct_chg_diff}")
                #         self.say(name + f"急拉{pct_chg_diff}")
                #         self.set_record_time(record_key)
                #快速打压
                # elif pct_chg_diff < down_pct_dff:
                #     if not self.is_recorded(record_key, 5):
                #         record_key = notify_code + ", fast_down"
                #         self.dingding(record_key)
                #         self.log(name + f"猛砸{pct_chg_diff}")
                #         self.say(name + f"猛砸{pct_chg_diff}")
                #         self.set_record_time(record_key)

                #撬板
                if data["low"] <= round(data["pre_close"]*(1 - limit_up_pct), 2) and data["close"] > data["low"]:
                    record_key = notify_code + ", has_up"
                    if not self.is_recorded(record_key, 600):
                        #self.dingding(record_key)
                        self.log(name + "撬板")
                        self.say(name + "撬板")
                        self.set_record_time(record_key)

                #炸板
                if data["high"] >= round(data["pre_close"]*(1 + limit_up_pct), 2) and data["close"] < data["high"]:
                    record_key = notify_code + ", has_down"
                    if not self.is_recorded(record_key, 600):
                        #self.dingding(record_key)
                        self.log(name + "炸板")
                        self.say(name + "炸板")
                        self.set_record_time(record_key)

                pct_chg = round(data["pct_chg"], 1)
                #大涨
                if up_pct != 0 and pct_chg > up_pct:
                    record_key = notify_code + ", out_up"
                    if not self.is_recorded(record_key, 3600):
                        self.dingding(record_key)
                        self.log(name + f"暴涨{pct_chg}")
                        self.say(name + f"暴涨{pct_chg}")
                        self.set_record_time(record_key)
                #大跌
                elif down_pct != 0 and pct_chg < down_pct:
                    record_key = notify_code + ", out_down"
                    if not self.is_recorded(record_key, 3600):
                        self.dingding(record_key)
                        self.log(name + f"暴跌{pct_chg}")
                        self.say(name + f"暴跌{pct_chg}")
                        self.set_record_time(record_key)

            #成交量激增
            record_key = notify_code + ", v_up"
            avp_volume = sum(map(float, self.volume_diff_list[code]))/len(self.volume_diff_list[code])
            volume_diff = data["volume"] - self.last_volume[code]
            #if not self.is_recorded(record_key, 5) and volume_diff > avp_volume * 4 and volume_diff > min_volume_diff:
            if not self.is_recorded(record_key, 5) and volume_diff*data["close"]*100 > min_volume_diff:
                #self.dingding(record_key)
                vd = str(volume_diff)[0] + "0"*(len(str(volume_diff))-1)
                self.log(name + f"成交{vd}")
                self.say(name + f"成交{vd}")
                self.set_record_time(record_key)

            #if not self.is_recorded(code, 5):
            #    self.__set_last_data(code, data)
            #    self.set_record_time(code)
            self.__set_last_data(code, data)
        except Exception as e:
            err_log()
            core.say("子线程发生错误" + code)
            core.dingding("子线程发生错误")

    ban_code_list = []
    def __set_last_data(self, code, data):
        volume_diff = data["volume"]
        if code in self.last_volume:
            volume_diff = data["volume"] - self.last_volume[code]
        if code not in self.volume_diff_list:
            self.volume_diff_list[code] = []
        if len(self.volume_diff_list[code]) >=  10:
            self.volume_diff_list[code].pop(0)
        self.volume_diff_list[code].append(volume_diff)

        self.last_volume[code] = data["volume"]
        self.last_buy1_lots[code] = data["buy1_lots"]
        self.last_sell1_lots[code] = data["sell1_lots"]
        self.last_pct_chg[code] = data["pct_chg"]

    def __get_code_list(self, filename):
        with open(filename, 'r') as f:
            lines = f.readlines()
        code_list = []
        for code in lines:
            code = code.replace("SH", "").replace("SZ", "").strip()[:6]
            if code and len(code) >= 6 and code[0].isdigit():
                code_list.append(code)
        return list(set(code_list))

    def scan_up_stock(self):
        code_list = self.__get_code_list(os.path.abspath(os.path.dirname(__file__)) + "/conf/up_code_list.txt")
        if len(code_list) > 0:
            pool = ThreadPoolExecutor(min(40, len(code_list)))
            for code in code_list:
                pool.submit(self.__scan_up_stock, code)
            pool.shutdown()

    def scan_bond(self):
        code_list = self.__get_code_list(os.path.abspath(os.path.dirname(__file__)) + "/conf/bond_code_list.txt")
        if len(code_list) > 0:
            pool = ThreadPoolExecutor(min(40, len(code_list)))
            for code in code_list:
                pool.submit(self.__scan_up_stock, code, down_pct_dff = -0.5, up_pct_diff = 0.5, min_volume_diff = 500)
            pool.shutdown()

    def scan_weight_stock(self):
        code_list = self.__get_code_list(os.path.abspath(os.path.dirname(__file__)) + "/conf/weight_code_list.txt")
        if len(code_list) > 0:
            pool = ThreadPoolExecutor(min(40, len(code_list)))
            for code in code_list:
                pool.submit(self.__scan_up_stock, code, up_pct = 1, down_pct = -1)
            pool.shutdown()

    def report_market(self):
        now = datetime.datetime.now()
        #now = datetime.datetime.strptime(str(now.date()) + " 09:45", "%Y-%m-%d %H:%M")
        today_date = str(now.date())
        ta = datetime.datetime.strptime(today_date + " 09:25", "%Y-%m-%d %H:%M")
        tb = datetime.datetime.strptime(today_date + " 09:40", "%Y-%m-%d %H:%M")
        tc = datetime.datetime.strptime(today_date + " 10:30", "%Y-%m-%d %H:%M")
        record_key = "report_market"
        if now > tc and not self.is_recorded(record_key + "up1030", 43200):
            record_key  = record_key + "up1030"
        elif now > tb and not self.is_recorded(record_key + "up0940", 43200):
            record_key  = record_key + "up0940"
        elif now > ta and not self.is_recorded(record_key + "up0925", 43200):
            record_key  = record_key + "up0925"

        if record_key != "report_market":
            df = ak.stock_zt_pool_em(now.strftime("%Y%m%d"))
            df = df[df.apply(lambda x:x["代码"].find("300") != 0 and x["代码"].find("688") != 0 and x["名称"].find("退") == -1 and x["名称"].lower().find("st") == -1, axis=1)]
            ranks = [str(row) for row in df["所属行业"].value_counts()[0:5].items()]
            self.say("当前涨停数", str(df.index.size), ".", str.join("..", ranks))
            self.set_record_time(record_key)

        record_key = "report_market"
        if now > ta and not self.is_recorded(record_key + "down0925", 43200):
            df = ak.stock_zt_pool_dtgc_em(now.strftime("%Y%m%d"))
            df = df[df.apply(lambda x:x["代码"].find("300") != 0 and x["代码"].find("688") != 0 and x["名称"].find("退") == -1 and x["名称"].lower().find("st") == -1, axis=1)]
            ranks = [row["名称"] for i, row in df.iterrows()]
            self.say("跌停数", str(df.index.size), str.join(".", ranks))
            self.set_record_time(record_key + "down0925")


parser = argparse.ArgumentParser(description='股票监控')
# parser.add_argument('--project', '-p', help='项目值, 可选值:\n up:打板监控, all:全部. 默认为全部', default="all", choices=["all", "ban"])
parser.add_argument('project', help='项目值, 可选值:\n up:打板监控, weight:风向股, bond:可转债, buying:准备买入的股票, etf:etf基金, report:行情实时播报. 默认为etf', default="etf", choices=["etf", "up", "weight", "bond", "buying", "report"])
parser.add_argument('is_dev', help='是否为调试模式, 可选值:\n 0:否, 1:是. 默认为0', default=0,  type = int, choices=[0, 1])
args = parser.parse_args()
if __name__ == '__main__':
    # data = pd.read_excel("/Users/chen/Downloads/kk.xlsx", usecols=[0], skiprows=[0], names = ['代码']).iterrows()
    # hold_code_list = []
    # for i, v in data:
    #     print("\"" + v["代码"][2:] + "\", \n", )

    is_dev = args.is_dev
    core = Monitor()
    while True:
        now = datetime.datetime.now()
        today_date = str(now.date())
        begin = datetime.datetime.strptime(today_date + " 09:30", "%Y-%m-%d %H:%M")
        end = datetime.datetime.strptime(today_date + " 11:30", "%Y-%m-%d %H:%M")
        begin2 = datetime.datetime.strptime(today_date + " 13:00", "%Y-%m-%d %H:%M")
        end2 = datetime.datetime.strptime(today_date + " 14:56", "%Y-%m-%d %H:%M")
        if is_dev == 1 or (now.weekday() in range(0, 5) and (begin <= now <= end or begin2 <= now <= end2)):
            try:
                if args.project == "etf":
                    core.scan_etf()
                elif args.project == "up":
                    core.scan_up_stock()
                elif args.project == "weight":
                    core.scan_weight_stock()
                elif args.project == "bond":
                    core.scan_bond()
                elif args.project == "buying":
                    core.scan_buying()
                elif args.project == "report":
                    core.report_market()
            except Exception:
                err_log()
                core.say("发生错误")
                core.dingding("发生错误")
        time.sleep(1)
