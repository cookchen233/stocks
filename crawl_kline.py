import argparse
import sys

import baostock as bs
import pandas as pd

from tool import *
from model.connecter import *
import stock

import akshare as ak
from pytdx.hq import TdxHq_API
from multiprocessing import Manager, Pool
import os
from sqlalchemy import desc, func, cast
from datetime import datetime, time, timedelta
from time import sleep
from subprocess import call


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
        # print('login respond error_code:' + lg.error_code)
        # print('login respond  error_msg:' + lg.error_msg)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tdx.disconnect()
        self.db.session.remove()
        self.baostock.logout()

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

    def filter_klines(self, code, klines):
        query = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.code == code,
            func.date(MinuteKlines.close_time) == klines[0].close_time.date()
            ])
        records = query.all()

        filtered_klines = []
        for kline in klines:
            ex = False
            for record in records:
                if record.close_time == kline.close_time:
                    ex = True
                    break
            if not ex:
                filtered_klines.append(kline)
        return filtered_klines

    def __get_last_day_time(self, date):
        start_of_day = datetime.combine(date, datetime.min.time())
        end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
        return start_of_day, end_of_day

    def __get_last_kline(self, kline: MinuteKlines):
        last_day = before_dates((kline.close_time - timedelta(days=1)), 1)[0]
        last_day_kline = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.code == kline.code,
            # func.date(MinuteKlines.close_time) == (kline.close_time - timedelta(days=1)).date()
            MinuteKlines.close_time == last_day.replace(hour=15, minute=0)
        ]).group_by(
            MinuteKlines.code
        ).order_by(sqlalchemy.desc(MinuteKlines.close_time))

        obj = last_day_kline.statement.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        # print(str(obj ))

        return last_day_kline.first()

    def save_history_klines(self, date, code):
        save_code = get_code_market2(code).upper()+code
        ex = self.db.session.query(MinuteKlines).filter(*[
            MinuteKlines.code == save_code,
            func.date(MinuteKlines.close_time) == date.date()
            ]).order_by(sqlalchemy.asc(MinuteKlines.close_time)) .all()
        if len(ex) > 0:
            if len(ex) == 49 and ex[0].close_time.strftime("%H:%M") == "09:30" and ex[-1].close_time.strftime("%H:%M") == "15:00":
                print(date, save_code, "已存在")
                return False
            else:
                pass
                raise ValueError(str(date.date()) + " " + save_code + " 已有数据异常")
        klines = self.crawl_history_klines(date, save_code)
        if len(klines) > 0:
            self.db.session.add_all(self.filter_klines(save_code, klines))
            self.db.session.commit()
        return True

    def crawl_history_klines(self, date, code):
        symbol_code = code[0:2].lower() + "." + code[2:]
        static_info = self.stock.get_static_info(code)
        # print(static_info,static_info["流通股"], static_info["自由流通股"], static_info["股比"])
        # return

        # 日线, 用于获取昨日收盘价
        fields = "date,code,open,high,low,close,preclose,volume"
        try:
            day_kline = self.baostock.query_history_k_data_plus(symbol_code, fields, start_date=str(date.date()), end_date=str(date.date()), frequency="d", adjustflag="3").get_data().iloc[0]
        except Exception as e:
            print(date.date(), code, " 获取日线错误")
            raise e
        if not day_kline["volume"]:
            print(date.date(), code, "停牌")
            return []

        data_list = []
        open = float(day_kline["open"])
        preclose = float(day_kline["preclose"])
        data = MinuteKlines(
            code=code,
            name=static_info["名称"],
            close_time = datetime.combine(date, time(9, 30)),
            open=preclose,
            close=open,
            pct_chg=(open - preclose) / preclose * 100,
            day_pct_chg=(open - preclose) / preclose * 100,
            day_pct_chg2=(open - preclose) / preclose * 100,
            day_market_value=open * static_info["自由流通股"],
        )

        # 连续涨停或大涨
        last_kline = self.__get_last_kline(data)
        limit_up_days = last_kline.limit_up_days if last_kline else 0
        up_days = last_kline.up_days if last_kline else 0
        if data.close >= round(preclose * 1.1, 2):
            data.limit_up_days = limit_up_days + 1
        if data.day_pct_chg >=5:
            data.up_days = up_days + 1

        data_list.append(data)

        # 5分钟线
        fields = "date,time,code,open,high,low,close,volume,amount"
        try:
            klines = self.baostock.query_history_k_data_plus(symbol_code, fields, start_date=str(date.date()), end_date=str(date.date()), frequency="5", adjustflag="3").get_data()
        except Exception as e:
            print(str(date.date()), code, " 获取分钟线错误")
            raise e

        day_volume = 0
        day_amount = 0
        for i, kline in klines.iterrows():
            k_time = datetime.strptime(kline["time"], "%Y%m%d%H%M%S%f")
            if k_time.date() != date.date():
                print(date.date(), code, "k线日期与指定日期不相符", k_time)
                return []
            k_open = float(kline["open"])
            k_close = float(kline["close"])
            k_volume = float(kline["volume"])
            k_amount = float(kline["amount"])
            day_volume += k_volume
            day_amount += k_amount
            data = MinuteKlines(
                code=code,
                name=static_info["名称"],
                close_time=k_time,
                open=k_open,
                high=kline["high"],
                low=kline["low"],
                close=k_close,
                pct_chg=(k_close - k_open) / k_open * 100,
                volume=k_volume,
                amount=kline["amount"],
                day_pct_chg=(k_close - preclose) / preclose * 100,
                day_pct_chg2=(k_close - open) / open * 100,
                day_turnover=day_volume / static_info["自由流通股"] * 100,
                day_amount=day_amount,
                day_market_value=k_close * static_info["自由流通股"],
            )
            if data.close >= round(preclose * 1.1, 2):
                data.limit_up_days = limit_up_days + 1
            if data.day_pct_chg >=5:
                data.up_days = up_days + 1

            data_list.append(data)

        return data_list

    def save_live_klines(self, code):
        if not self.stock.right_time("09:30", "12:00") and not self.stock.right_time("12:00", "15:30"):
            print("非交易时间")
            return False
        klines = self.crawl_live_klines(code)
        if len(klines) == 0:
            print("抓取数据为空")
            return False
        self.db.session.add_all(self.filter_klines(code, klines))
        self.db.session.commit()
        return True

    def crawl_live_klines(self, code):
        static_info = self.stock.get_static_info(code)
        if static_info is None:
            return []
        # 当前数据, 用于获取昨日收盘价
        newest = self.stock.get_live_data(code)
        data_list = []
        open = newest["open"]
        preclose = newest["pre_close"]
        date = before_dates(datetime.now(), 1)[0]

        data = MinuteKlines(
            code=code,
            name=newest["name"],
            close_time = datetime.combine(date, time(9, 30)),
            open=preclose,
            close=open,
            pct_chg=(open - preclose) / preclose * 100,
            volume=0,
            amount=0,
            day_pct_chg=(open - preclose) / preclose * 100,
            day_pct_chg2=(open - preclose) / preclose * 100,
            day_market_value=open * static_info["自由流通股"]
        )
        # 连续涨停或大涨
        last_kline = self.__get_last_kline(data)
        limit_up_days = last_kline.limit_up_days if last_kline else 0
        up_days = last_kline.up_days if last_kline else 0
        if data.close >= round(preclose * 1.1, 2):
            data.limit_up_days = limit_up_days + 1
        if data.day_pct_chg >=5:
            data.up_days = up_days + 1
        data_list.append(data)

        # 1分钟线
        klines = ak.stock_zh_a_hist_min_em(code.replace("SH","").replace("SZ", ""), start_date=str(date.date()), period="5")
        day_volume = 0
        day_amount = 0
        for i, kline in klines.iterrows():
            k_time = datetime.strptime(kline["时间"], "%Y-%m-%d %H:%M:%S")
            if k_time.date() != date.date():
                print(date.date(), code, "k线日期与指定日期不相符", k_time)
                return []
            if i == 0 and k_time.strftime("%H:%M") == "09:30":
                continue

            k_open = float(kline["开盘"])
            k_close = float(kline["收盘"])
            k_volume = float(kline["成交量"])
            k_amount = float(kline["成交额"])
            day_volume += k_volume
            day_amount += k_amount
            data = MinuteKlines(
                code=code,
                name=newest["name"],
                close_time=k_time,
                open=k_open,
                high=kline["最高"],
                low=kline["最低"],
                close=k_close,
                pct_chg=(k_close - k_open) / k_open * 100,
                volume=kline["成交量"],
                amount=kline["成交额"],
                day_pct_chg=(k_close - preclose) / preclose * 100,
                day_pct_chg2=(k_close - open) / open * 100,
                day_turnover=day_volume / static_info["自由流通股"] * 100,
                day_amount=day_amount,
                day_market_value=k_close * static_info["自由流通股"],
            )
            if data.close >= round(preclose * 1.1, 2):
                data.limit_up_days = limit_up_days + 1
            if data.day_pct_chg >=5:
                data.up_days = up_days + 1

            data_list.append(data)

        return data_list


parser = argparse.ArgumentParser(description='实时k线数据保存')
parser.add_argument('crawl_type', help='抓取类型, history: 历史k线, live: 当日实时获取', type=str, choices=["history", "live"])
args = parser.parse_args()

def process_err_callback(err):
    print(f'子进程发生错误：{str(err)}')

ck = CrawlKline()
def process_save_history_klines(row, dates, tasks_completed, total_tasks, lock):
    print("子进程执行开始", row["股票简称"])
    try:
        for date in dates:
            ck.save_history_klines(date, row["股票代码"].replace(".SH","").replace(".SZ",""))
        with lock:
            tasks_completed.value += 1
            remaining_tasks = total_tasks.value - tasks_completed.value
            print(row["股票简称"], "完成", f"剩余任务数量: {remaining_tasks}")
    except Exception as e:
        err_log()
        raise e

def process_save_live_klines(code, tasks_completed, total_tasks, lock):
    print("子进程执行开始", code)
    try:
        if ck.save_live_klines(code):
            with lock:
                tasks_completed.value += 1
                remaining_tasks = total_tasks.value - tasks_completed.value
                print(code, "完成", f"剩余任务数量: {remaining_tasks}")
    except Exception as e:
        err_log()
        raise e

if __name__ == '__main__':

    crawl_type = args.crawl_type

    if crawl_type == "history":
        script_directory = os.path.dirname(os.path.realpath(__file__))
        filename = os.path.join(script_directory, "conf/stock_static_info.xlsx")
        filename = os.path.join(script_directory, "conf/risk.xlsx")
        df = pd.read_excel(filename)
        dates = range_dates(datetime.strptime("2024-04-24", "%Y-%m-%d"),  datetime.strptime("2024-04-24", "%Y-%m-%d"))
        # for index, row in df.iterrows():
        #     print(row["名称"])
        #     for date in dates:
        #         ck.save_history_klines(date,  row["代码"])
        # sys.exit()

        # 使用 Manager 来创建共享变量
        manager = Manager()
        total_tasks = manager.Value("i", len(df))
        tasks_completed = manager.Value("i", 0)
        lock = manager.Lock()

        # 创建进程池
        pool = Pool(processes=5)  # 假设您希望同时执行的最大进程数是 5

        # 处理每一行
        for index, row in df.iterrows():
            pool.apply_async(process_save_history_klines, args=(row, dates, tasks_completed, total_tasks, lock), error_callback=process_err_callback)

        # 关闭进程池，等待所有进程完成
        pool.close()
        pool.join()
        print("所有任务执行完毕")

    elif crawl_type == "live":
        while True:
            try:
                cur_min = int(datetime.now().strftime("%M"))
                if cur_min % 5 != 0:
                    print("等待")
                    sleep(1)
                    continue
                code_list = get_code_list(os.path.join(os.path.abspath(os.path.dirname(__file__)), "conf/risk.txt"))
                # for code in code_list:
                #     ck.save_live_klines(code)
                # 使用 Manager 来创建共享变量
                manager = Manager()
                total_tasks = manager.Value("i", len(code_list))
                tasks_completed = manager.Value("i", 0)
                lock = manager.Lock()
                # 创建进程池
                pool = Pool(processes=5)  # 假设您希望同时执行的最大进程数是 5
                # 处理每一行
                for code in code_list:
                    pool.apply_async(process_save_live_klines, args=(code, tasks_completed, total_tasks, lock), error_callback=process_err_callback)

                # 关闭进程池，等待所有进程完成
                pool.close()
                pool.join()
                print(datetime.now(), "所有任务执行完毕")
            except Exception as e:
                err_log()
                sleep(60)
                call(["python3", os.path.abspath(os.path.dirname(__file__)) + "/speak.py", "抓取情绪数据发生错误"])
