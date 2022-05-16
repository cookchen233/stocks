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

class Gds(object):

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

    def get_gds(self, filter = "20210930", sort_column = "INTERVAL_CHRATE", sort_type = "1"):
        """
        东方财富网-数据中心-特色数据-股东户数
        http://data.eastmoney.com/gdhs/
        :param symbol: choice of {"最新", 每个季度末}
        :type symbol: str
        :return: 股东户数
        :rtype: pandas.DataFrame
        """
        url = "http://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "sortColumns": sort_column, #"HOLD_NOTICE_DATE,SECURITY_CODE",
            "sortTypes": sort_type, #"-1,-1",
            "pageSize": 500,
            "pageNumber": 1,
            "reportName": "RPT_HOLDERNUMLATEST",
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,END_DATE,PRE_END_DATE",
            "quoteColumns": "f2,f3",
            "source": "WEB",
            "client": "WEB",
            "filter": f"{filter}"#f"(END_DATE='{symbol[:4] + '-' +  symbol[4:6] + '-' +  symbol[6:]}')",
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["result"]:
            return []
        data = data_json["result"]["data"]
        for page in range(2, data_json["result"]["pages"] + 1):
            params["pageNumber"] = page
            r = requests.get(url, params=params)
            data_json = r.json()
            data = data + data_json["result"]["data"]
        return data


    def run(self, hold_dec_pct, hold_dec_num, pct, date = ""):

        data = self.get_gds(filter = "(HOLDER_NUM_RATIO<{})(INTERVAL_CHRATE<{}){}".format(hold_dec_pct, pct, date, f"(END_DATE='{date}')" if date != "" else ""), sort_column = "HOLDER_NUM_RATIO", sort_type = "1")
        d = []
        sum_one_m = 0
        sum_two_m = 0
        sum_three_m = 0
        sum_four_m = 0
        sum_five_m = 0
        sum_six_m = 0

        for v in data:
            if "st" in v["SECURITY_NAME_ABBR"][-1] == "退" or v["SECURITY_NAME_ABBR"][-1][:2] == "退市" or "st" in v["SECURITY_NAME_ABBR"] or "ST" in v["SECURITY_NAME_ABBR"]:
                continue
            if abs(int(v["HOLDER_NUM_CHANGE"])) < hold_dec_num:
                continue
            if v["SECURITY_CODE"][:3] == "300" or v["SECURITY_CODE"][:3] == "688":
                continue
            if v["TOTAL_MARKET_CAP"] > 100000000:
                continue
            if datetime.datetime.strptime(v["END_DATE"],  "%Y-%m-%d %H:%M:%S") <= datetime.datetime.strptime("2021-09-30",  "%Y-%m-%d"):
                continue
            stock_daily = self.em_stock_daily(v["SECURITY_CODE"])
            if stock_daily.index.size < 300:
                continue
            stock_daily = self.em_stock_daily(v["SECURITY_CODE"], beg = v["END_DATE"][:10].replace("-", ""))
            #(datetime.datetime.strptime(v["END_DATE"],  "%Y-%m-%d %H:%M:%S")  +  datetime.timedelta(days=90)).strftime("%Y%m%d")
            hold_value = int(v["AVG_MARKET_CAP"])
            # if hold_value>10000:
            #     hold_value = str(round(hold_value/10000, 2)) + "万"
            holder_num = int(v["HOLDER_NUM"])
            # if holder_num>10000:
            #     holder_num = str(round(holder_num/10000, 2)) + "万"
            pre_holder_num = int(v["PRE_HOLDER_NUM"])
            # if pre_holder_num>10000:
            #     pre_holder_num = str(round(pre_holder_num/10000, 2)) + "万"
            dec_num = int(v["HOLDER_NUM_CHANGE"])
            # if abs(dec_num)>10000:
            #     dec_num = str(round(dec_num/10000, 2)) + "万"

            one_m = (stock_daily.iloc[20]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>20 else 0
            two_m = (stock_daily.iloc[40]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>40 else 0
            three_m = (stock_daily.iloc[60]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>60 else 0
            four_m = (stock_daily.iloc[80]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>80 else 0
            five_m = (stock_daily.iloc[100]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>100 else 0
            six_m = (stock_daily.iloc[120]["close"] - stock_daily.iloc[0]["close"])/stock_daily.iloc[0]["close"]*100 if stock_daily.index.size>120 else 0
            sum_one_m = sum_one_m + one_m
            sum_two_m = sum_two_m + two_m
            sum_three_m = sum_three_m + three_m
            sum_four_m = sum_four_m + four_m
            sum_five_m = sum_five_m + five_m
            sum_six_m = sum_six_m + six_m
            d.append({
                "代码": v["SECURITY_CODE"],
                "名称": v["SECURITY_NAME_ABBR"],
                "户均持(元)": int(hold_value),
                "本次(户)": int(holder_num),
                "上次(户)": int(pre_holder_num),
                "增减(户)": int(dec_num),
                "增减比(%)": round(float(v["HOLDER_NUM_RATIO"]), 2),
                "上次统计": v["PRE_END_DATE"][:10],
                "本次统计": v["END_DATE"][:10],
                "上本涨跌(%)" :round(float(v["INTERVAL_CHRATE"]), 2),
                "1个月后(%)": round(one_m, 2),
                "2个月后(%)": round(two_m, 2),
                "3个月后(%)": round(three_m, 2),
                "4个月后(%)": round(four_m, 2),
                "5个月后(%)": round(five_m, 2),
                "6个月后(%)": round(six_m, 2),
            })
        if len(d)<1:
            return (0, )
        else:
            avp_one_m = round(sum_one_m/len(d), 2)


            avp_two_m = round(sum_two_m/len(d), 2)


            avp_three_m = round(sum_three_m/len(d), 2)


            avp_four_m = round(sum_four_m/len(d), 2)


            avp_five_m = round(sum_five_m/len(d), 2)


            avp_six_m = round(sum_six_m/len(d), 2)

            d.append({
                "代码": "平均统计",
                "1个月后(%)": avp_one_m,
                "2个月后(%)": avp_two_m,
                "3个月后(%)": avp_three_m,
                "4个月后(%)": avp_four_m,
                "5个月后(%)": avp_five_m,
                "6个月后(%)": avp_six_m,
            })
            dataFrame = pd.DataFrame(d)
            filename = "gds.xlsx"
            mode = "a"
            if_sheet_exists = "replace"
            if not os.path.exists(filename):
                mode = "w"
                if_sheet_exists = None
            with pd.ExcelWriter("./data/gds.xlsx", if_sheet_exists=if_sheet_exists, mode=mode) as writer:
                dataFrame.to_excel(writer, sheet_name=f"{date}" if date != "" else "最新", float_format="%.6f")
            return (len(d), avp_one_m, avp_two_m, avp_two_m, avp_three_m, avp_four_m, avp_five_m)


if __name__ == '__main__':
    core = Gds()
    dates = [
        # "2019-12-31",
        # "2020-03-31",
        # "2021-06-30",
        # "2021-09-30",
        # "2020-12-31",
        # "2021-03-31",
        "2021-06-30",
        "2021-09-30",
        "2021-12-31",
    ]
    for date in dates:
        result = core.run(-10, 5000, 30, date)
        # result = core.run(-1, 500, 100, "")

