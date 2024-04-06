#!/usr/local/bin/python3

import argparse
import queue
import threading
from subprocess import call

import baostock as bs
import pandas as pd
import math, datetime, requests, re, json, time, random, pandas, hashlib, sklearn, numpy

import pyttsx3
from bs4 import BeautifulSoup
from matplotlib import ticker
from pyttsx3 import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, between
import numpy as np
import matplotlib.pyplot as plt
import time

from sqlalchemy.orm import aliased

from tool import *
from model.connecter import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed
import easyquotation
from PIL import ImageGrab

import akshare as ak
import counter
from openpyxl import load_workbook


class Stock(object):

    static_info = {}
    db = None

    def __init__(self):
        self.db = DataBase()
        self.session = requests.Session()
        self.get_live_data = retry_decorator(self.get_live_data)
        ak.stock_zh_index_daily_em = retry_decorator(ak.stock_zh_index_daily_em)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __stock_real_time(self, stock: str = "600094", market: str = "sh"):
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
        json_data = json.loads(text_data[text_data.find("{"): -2])
        data = json_data["data"]
        return {
            "代码": data["f57"],
            "名称": data["f58"],
            "最高": float(data["f44"]) if data["f44"] != "-" else float(data["f60"]),
            "最低": float(data["f45"]) if data["f45"] != "-" else float(data["f60"]),
            "最新价": float(data["f43"]) if data["f43"] != "-" else float(data["f60"]),
            "开盘": float(data["f46"]) if data["f46"] != "-" else float(data["f60"]),
            "昨收": float(data["f60"]),
            "涨跌幅": float(data["f170"]),
            "主力净额": float(data["f137"]),
            "主力净比": float(data["f193"]),
            "量比": float(data["f50"]),
            "换手": float(data["f168"]),
            "市盈率": float(data["f162"]),
        }

    def get_live_data(self, code):
        secid = code.replace("SH", "1.").replace("SZ", "0.")
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        }
        params = {
            "fltt": "2",
            "invt": "2",
            "klt": "1",
            "secid": secid,
            "fields": "f57,f58,f60,f43,f44,f45,f47,f170,f19,f20,f39,f40,f530,f168,f46",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            # "cb": "jQuery183003743205523325188_1589197499471",
            "_": int(time.time() * 1000),
        }
        r = self.session.get(url, params=params, headers=headers)
        data = json.loads(r.text)["data"]
        if not data:
            print(code)
        turnover = float(data["f168"]) if data["f168"] != "-" else float(data["f168"])
        static_info = self.get_static_info(code)
        if static_info:
            real_turnover_ratio = float(static_info["股比"])
        else:
            real_turnover_ratio = 1.0

        return {
            "code": data["f57"],
            "name": data["f58"],
            "pre_close": float(data["f60"]),
            "open": float(data["f46"]) if data["f46"] != "-" else float(data["f60"]),
            "close": float(data["f43"]) if data["f43"] != "-" else float(data["f60"]),
            "high": float(data["f44"]) if data["f44"] != "-" else float(data["f60"]),
            "low": float(data["f45"]) if data["f45"] != "-" else float(data["f60"]),
            "pct_chg": float(data["f170"]) if data["f170"] != "-" else 0,
            "buy1_lots": int(data["f20"]) if data["f20"] != "-" else 0,
            "sell1_lots": int(data["f40"]) if data["f40"] != "-" else 0,
            "volume": int(data["f47"]) if data["f47"] != "-" else 0,
            "turnover": turnover,
            "real_turnover": turnover * real_turnover_ratio,
            "real_turnover_ratio": real_turnover_ratio,
        }

    def get_static_info(self, code):
        if code in self.static_info:
            return self.static_info[code]

        script_directory = os.path.dirname(os.path.realpath(__file__))
        filename = os.path.join(script_directory, "conf/stock_static_info.xlsx")
        df = pd.read_excel(filename)

        # 查找某一列为特定值的数据
        column_name = "代码"  # 列名
        search_value = code  # 要查找的值
        filtered_data = df[df[column_name] == search_value]
        data = pd.DataFrame(filtered_data).to_dict(orient='records')
        if data:
            self.static_info[code] = data[0]
            self.static_info[code]["流通股"] = unit_to_int(data[0]["流通股"])
            self.static_info[code]["自由流通股"] = unit_to_int(data[0]["自由流通股"])
            return self.static_info[code]
        return None

    def right_time(self, begin_time, end_time):
        now_time=datetime.now()
        today_date=str(now_time.date())
        begin=datetime.strptime(today_date + " "+begin_time, "%Y-%m-%d %H:%M")
        end=datetime.strptime(today_date + " "+end_time, "%Y-%m-%d %H:%M")
        return now_time.weekday() in range(0, 5) and (begin <= now_time <= end)

    # 连续两天涨停
    def get_continuous_limit_up_stocks(self, date, limit_up_days, limit_up_range_days):
        dates = before_dates(date -  timedelta(days=1), limit_up_range_days)
        results = (self.db.session.query(MinuteKlines)
        .filter(
            MinuteKlines.up_days >= limit_up_days,
            func.extract('hour', MinuteKlines.close_time) == 15,
            between(func.date(MinuteKlines.close_time), dates[0].date(), dates[-1].date())
        ).group_by(MinuteKlines.code))
        obj = results.statement.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        # print(str(obj ))
        return results.all()


