import argparse

import baostock as bs
import pandas as pd
import math, datetime, requests, re, json, time, random, pandas, hashlib, sklearn, numpy
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
import stock

import akshare as ak
from pytdx.hq import TdxHq_API
from sqlalchemy import desc


class CrawlKline(object):
    stock = None
    tdx = None
    db = None
    baostock = None

    def __init__(self):
        ak.stock_zh_a_hist_min_em = retry_decorator(ak.stock_zh_a_hist_min_em)

        self.stock = stock.Stock()

        self.tdx = TdxHq_API()
        self.tdx.connect('119.147.212.81', 7709)

        self.db = DataBase()

        self.baostock = bs
        lg = self.baostock.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tdx.disconnect()
        self.db.session.remove()
        self.baostock.logout()

    def save_stock_klines2(self, code, date_str):
        db_data = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.date == date_str,
            MinuteKlines.code == code,
        ]).first()
        if db_data:
            print("已存在", code, date_str)
            return

        try:
            # 查询股票5分钟k线数据
            bars = self.tdx.get_security_bars(0, get_code_market(code), code, calculate_value(date_str), 49)
            pre_bar = bars.pop(0)
            first_bar = bars[0]
            for v in bars:
                if v["datetime"][0:10] != date_str:
                    print(date_str, code, "停牌", v["datetime"])
                    return
                data = MinuteKlines(
                    code=code,
                    name=code,
                    date=date_str,
                    time=v["datetime"],
                    open=v["open"],
                    high=v["high"],
                    low=v["low"],
                    close=v["close"],
                    pct_chg=(v["close"] - v["open"]) / v["open"] * 100,
                    day_pct_chg=(v["close"] - pre_bar["close"]) / pre_bar["close"] * 100,
                    day_pct_chg2=(v["close"] - first_bar["open"]) / first_bar["open"] * 100,
                    volume=v["vol"],
                    amount=v["amount"],
                )
                self.db.session.add(data)
                self.db.session.commit()

            data = MinuteKlines(
                code=code,
                name=code,
                date=date_str,
                time=date_str + " 09:30:00",
                open=pre_bar["close"],
                high=0,
                low=0,
                close=first_bar["open"],
                pct_chg=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
                volume=0,
                amount=0,
                day_pct_chg=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
                day_pct_chg2=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
            )

            self.db.session.add(data)
            self.db.session.commit()
        except Exception as e:
            print(date_str, code, "err===========")
            raise e

    def save_stock_m5(self, code, date_str):
        db_data = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.date == date_str,
            MinuteKlines.code == code,
        ]).first()
        if db_data:
            # print("已存在", code, date_str)
            return False
        try:
            klines = self.get_klines(code, date_str)
            if len(klines) > 0:
                self.db.session.add_all(klines)
                self.db.session.commit()
        except Exception as e:
            print("error", date_str, code)
            raise e
        return True

    def get_klines2(self, code, date_str):
        # 查询股票5分钟k线数据
        klines = self.tdx.get_security_bars(0, get_code_market(code), code, calculate_value(date_str), 49)
        pre_bar = klines.pop(0)
        first_bar = klines[0]
        data_list = []
        data = MinuteKlines(
            code=code,
            name=code,
            date=date_str,
            time=date_str + " 09:30:00",
            open=pre_bar["close"],
            high=0,
            low=0,
            close=first_bar["open"],
            pct_chg=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
            volume=0,
            amount=0,
            day_pct_chg=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
            day_pct_chg2=(first_bar["open"] - pre_bar["close"]) / pre_bar["close"] * 100,
        )
        data_list.append(data)
        for v in klines:
            if v["datetime"][0:10] != date_str:
                print(date_str, code, "k线时间与指定时间不相符", v["datetime"])
                return []
            data = MinuteKlines(
                code=code,
                name=code,
                date=date_str,
                time=v["datetime"],
                open=v["open"],
                high=v["high"],
                low=v["low"],
                close=v["close"],
                pct_chg=(v["close"] - v["open"]) / v["open"] * 100,
                day_pct_chg=(v["close"] - pre_bar["close"]) / pre_bar["close"] * 100,
                day_pct_chg2=(v["close"] - first_bar["open"]) / first_bar["open"] * 100,
                volume=v["vol"],
                amount=v["amount"],
            )
            data_list.append(data)

        return data_list

    def get_klines(self, code, date_str):
        symbol_code = get_code_market2(code) + "." + code
        # 日线, 用于获取昨日收盘价
        fields = "date,code,open,high,low,close,preclose,volume"
        print(self.baostock.query_history_k_data_plus(symbol_code, fields, start_date=date_str, end_date=date_str, frequency="d", adjustflag="3").get_data())
        day_kline = self.baostock.query_history_k_data_plus(symbol_code, fields, start_date=date_str, end_date=date_str, frequency="d", adjustflag="3").get_data().iloc[0]
        if not day_kline["volume"]:
            print(date_str, code, "停牌")
            return []

        data_list = []
        open = to_decimal(day_kline["open"])
        preclose = to_decimal(day_kline["preclose"])
        data = MinuteKlines(
            code=code,
            name=code,
            date=date_str,
            time=date_str + " 09:30:00",
            open=preclose,
            high=0,
            low=0,
            close=open,
            pct_chg=(open - preclose) / preclose * 100,
            volume=0,
            amount=0,
            day_pct_chg=(open - preclose) / preclose * 100,
            day_pct_chg2=(open - preclose) / preclose * 100,
        )
        data_list.append(data)

        # 5分钟线
        fields = "date,time,code,open,high,low,close,volume,amount"
        klines = self.baostock.query_history_k_data_plus(symbol_code, fields, start_date=date_str, end_date=date_str, frequency="5", adjustflag="3").get_data()

        for i, kline in klines.iterrows():
            k_time = datetime.strptime(kline["time"], "%Y%m%d%H%M%S%f").strftime("%Y-%m-%d %H:%M:%S")
            if k_time[0:10] != date_str:
                print(date_str, code, "k线时间与指定时间不相符", k_time)
                return []
            m5_open = float(kline["open"])
            m5_close = float(kline["close"])
            data = MinuteKlines(
                code=code,
                name=code,
                date=date_str,
                time=k_time,
                open=m5_open,
                high=to_decimal(kline["high"]),
                low=to_decimal(kline["low"]),
                close=m5_close,
                pct_chg=(m5_close - m5_open) / m5_open * 100,
                day_pct_chg=(m5_close - preclose) / preclose * 100,
                day_pct_chg2=(m5_close - open) / open * 100,
                volume=kline["volume"],
                amount=kline["amount"],
            )
            data_list.append(data)

        return data_list

    def save_stock_live_klines(self, code):
        if not self.stock.right_time("09:30", "11:30") and not self.stock.right_time("13:00", "15:00"):
            print("非交易时间")
            # return False
        last_record = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.code == code,
            ]).order_by(desc(MinuteKlines.trans_time)) .first()
        if last_record:
            if datetime.now() <= last_record.trans_time + timedelta(minutes=1):
                print("等待", code, last_record.trans_time)
                return False
        try:
            klines = self.get_live_minute_klines(code)

            if len(klines) > 0:
                self.db.session.add_all(klines)
                self.db.session.commit()
        except Exception as e:
            print("error", code)
            raise e
        return True

    def get_live_minute_klines(self, code):
        # 当前数据, 用于获取昨日收盘价
        newest = self.stock.get_live_data(code)
        data_list = []
        open = newest["open"]
        preclose = newest["pre_close"]
        date_str = datetime.now().strftime("%Y-%m-%d")
        time_str = datetime.now().strftime("%Y-%m-%d H%:M%:S%")
        data = MinuteKlines(
            create_time=time_str,
            code=code,
            name=newest["name"],
            trans_time=date_str + " 09:30:00",
            open=preclose,
            high=0,
            low=0,
            close=open,
            pct_chg=(open - preclose) / preclose * 100,
            volume=0,
            amount=0,
            day_pct_chg=(open - preclose) / preclose * 100,
            day_pct_chg2=(open - preclose) / preclose * 100,
            turnover=0,
            real_turnover=0,
        )
        data_list.append(data)

        # 1分钟线
        klines = ak.stock_zh_a_hist_min_em(code, start_date=date_str, period="1")
        for i, kline in klines.iterrows():
            k_time = kline["时间"]
            if k_time[0:10] != date_str:
                print(date_str, code, "k线时间与指定时间不相符", k_time)
                return []
            if "09:30:00" in k_time:
                continue

            k_open = kline["开盘"]
            k_close = kline["收盘"]
            data = MinuteKlines(
                create_time=time_str,
                code=code,
                name=newest["name"],
                trans_time=k_time,
                open=k_open,
                high=kline["最高"],
                low=kline["最低"],
                close=k_close,
                pct_chg=(k_close - k_open) / k_open * 100,
                day_pct_chg=(k_close - preclose) / preclose * 100,
                day_pct_chg2=(k_close - open) / open * 100,
                volume=kline["成交量"],
                amount=kline["成交额"],
                turnover=newest["turnover"],
                real_turnover=newest["real_turnover"],
            )
            data_list.append(data)

        return data_list


parser = argparse.ArgumentParser(description='实时k线数据保存')
parser.add_argument('dir', help='股票代码文件目录, ', type=str)
parser.add_argument('crawl_type', help='抓取类型, 1: 目录遍历, 2: 当日实时获取', type=int, choices=[1, 2])
args = parser.parse_args()
if __name__ == '__main__':

    dir = args.dir
    crawl_type = args.crawl_type

    ck = CrawlKline()
    if crawl_type == 1:
        dir = "/Volumes/[C] Windows 11/同花顺导出/risk2023-3"
        files_info = get_files(dir)
        print(files_info)
        for file_info in files_info:
            code_list = get_code_list(file_info['path'])
            saved = 0
            for code in code_list:
                if ck.save_stock_m5(code, file_info['name']):
                    saved += 1
            if saved > 0:
                print(file_info['name'], "保存成功")
    elif crawl_type == 2:
        while True:
            code_list = get_code_list(os.path.join(os.path.abspath(os.path.dirname(__file__)), dir))
            for code in code_list:
                if ck.save_stock_live_klines(code):
                    print(code, "保存成功")
            time.sleep(10)
