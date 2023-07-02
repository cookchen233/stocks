from subprocess import call

import baostock as bs
import pandas as pd
import math,datetime,requests,re,json,time,random,pandas,hashlib, sklearn, numpy

import pytz
from bs4 import BeautifulSoup
from matplotlib import ticker
from py_mini_racer import py_mini_racer
from selenium import webdriver
from selenium.webdriver import ChromeOptions

from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time

from tool import *
from model.connecter import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed
import easyquotation
import datetime
from chinese_calendar import is_workday
import requests_cache
from akshare.stock_feature.stock_wencai import *
import akshare as ak
from cacheout import LFUCache

class Wencai(object):

    def __init__(self):
        self.headers = load_header_string("""
Accept: application/json, text/javascript, */*; q=0.01
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Cache-Control: no-cache
Connection: keep-alive
Cookie: ta_random_userid=3qfu5ztacy; WafStatus=0; PHPSESSID=a43e4931827ab5bd3b0c17341eeaee0a; cid=a43e4931827ab5bd3b0c17341eeaee0a1656750788; ComputerID=a43e4931827ab5bd3b0c17341eeaee0a1656750788; user=MDpteF81ODg3MTgwNDc6Ok5vbmU6NTAwOjU5ODcxODA0Nzo1LDEsMTY7NiwxLDE2OzcsMTExMTExMTExMTEwLDE2OzgsMTExMTAxMTEwMDAwMTExMTEwMDEwMDEwMDEwMDAwMDAsMTY7MzMsMDAwMTAwMDAwMDAwLDE2OzM2LDEwMDExMTExMDAwMDExMDAxMDExMTExMSwxNjs0NiwwMDAwMTExMTEwMDAwMDExMTExMTExMTEsMTY7NTEsMTEwMDAwMDAwMDAwMDAwMCwxNjs1OCwwMDAwMDAwMDAwMDAwMDAwMSwxNjs3OCwxLDE2Ozg3LDAwMDAwMDAwMDAwMDAwMDAwMDAxMDAwMCwxNjs0NCwxMSw0MDsxLDEwMSw0MDsyLDEsNDA7MywxLDQwOzEwMiwxLDQwOjI0Ojo6NTg4NzE4MDQ3OjE2NTY4MTQ3NTc6OjoxNjI1NzI1ODAwOjIyOTI0MzowOjE0ZmQxYjUxMDIyMTBlMjdlZTE1ODc3MmI4MTQ2NmU1NDpkZWZhdWx0XzQ6MA%3D%3D; userid=588718047; u_name=mx_588718047; escapename=mx_588718047; ticket=5748040556aa8e2c06c8646a3f4739cf; user_status=0; utk=a9c07c17c44a4f56f6d3cbf3e27d10ab; v=AzfKU12AGc4omJ1ChrVC3DPCwCCC_A-H5d6P_YnlUVZtb1nWkcybrvWgHy6a
Host: www.iwencai.com
Pragma: no-cache
Referer: https://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=2022-06-29%E6%B6%A8%E5%B9%85%E4%B8%94%E7%AB%9E%E4%BB%B7%E6%B6%A8%E5%B9%85%E4%B8%94%E6%9C%80%E5%A4%A7%E6%B6%A8%E5%B9%85%E4%B8%94%E5%89%8D%E4%B8%80%E6%97%A5%E6%B6%A8%E5%B9%85%E4%B8%94%E8%BF%9E%E6%9D%BF%E6%95%B0%E4%B8%94%E5%AE%9E%E9%99%85%E6%8D%A2%E6%89%8B%E4%B8%94%E9%A6%96%E6%AC%A1%E6%B6%A8%E5%81%9C%E6%97%B6%E9%97%B4%E4%B8%94%E5%87%A0%E5%A4%A9%E5%87%A0%E6%9D%BF%EF%BC%8C%E4%B8%8A%E5%B8%82%E5%A4%A9%E6%95%B0%E5%A4%A7%E4%BA%8E100%EF%BC%8C%E6%B5%81%E9%80%9A%E5%B8%82%E5%80%BC%E5%B0%8F%E4%BA%8E80%E4%BA%BF%EF%BC%8C%E9%9D%9E%E9%80%80%E5%B8%82%EF%BC%8C%E9%9D%9Est%EF%BC%8C%E9%9D%9E%E5%88%9B%E4%B8%9A%E6%9D%BF%2C%20%E9%9D%9E%E7%A7%91%E5%88%9B%E6%9D%BF&queryarea=
sec-ch-ua: ".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "macOS"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36
X-Requested-With: XMLHttpRequest
        """)
        options = ChromeOptions()
        options.add_argument("--user-data-dir=/Users/chen/Library/Application Support/Google/Chrome")
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9014")
        self.browser = webdriver.Chrome(executable_path="/usr/local/bin/chromedriver", options=options)

        self.cache = LFUCache()

        #问财返回字段对应索引
        self.open_pct_col = 5
        self.close_pct_col = 4
        self.high_pct_col = 6
        self.cons_up_col = 7
        self.extra_col = 12
        self.turn_col = 8
        self.first_up_time_col = 9
        self.final_up_time_col = 10
        self.days_up_count = 10

        pytz.timezone('Asia/Chongqing')

        #重试包装
        requests.get = retry_decorator(requests.get)



    def  __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def ask(self, question, retry = 3):
        # js_code = py_mini_racer.MiniRacer()
        # with open("ths.js") as f:
        #     js_content = f.read()
        #     js_code.eval(js_content)
        #     v_code = js_code.call("v")

        url = "http://www.iwencai.com/stockpick/load-data?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&queryarea=&perpage=200"
        params = {
            "w":question,
        }
        cache_key = md5(url + question)
        resp_text = cache("wencai", cache_key)
        print(params)
        if resp_text is None:
            time.sleep(1)
            #session = requests_cache.CachedSession('demo_cache')
            resp = requests.get(url, params=params, headers = self.headers)
            print(resp.status_code)
            if resp.status_code == 403:
                if retry == 0:
                    raise Exception(resp.text)
                #采用浏览器内核重新获取 cookie
                self.browser.get("https://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=2022-06-29涨幅且竞价涨幅且最大涨幅且前一日涨幅且连板数且实际换手且首次涨停时间且几天几板，上市天数大于100，流通市值小于80亿，非退市，非st，非创业板, 非科创板&queryarea=&perpage=200")
                self.headers["Cookie"] = self.browser.execute_script("return document.cookie")
                return self.ask(question, retry-1)
            resp_text = resp.text
        try:
            data_json = json.loads(resp_text)
        except Exception as e:
            print(resp_text)
            raise e
        if data_json["success"] != True:
            raise Exception(resp.url, data_json["message"])

        cache("wencai", cache_key, resp_text)
        return data_json["data"]["result"]

    def trade_date_list(self, start_date_str, end_date_str, is_prev_day = False):
        dates = pd.bdate_range(start_date_str, end_date_str)
        dates = [x for x in dates if is_workday(x)]
        return dates

    #竞价跌停数
    def down_count(self, date_str):
        result = self.ask("{} 9:30跌停，上市天数大于100，非退市，非st，非创业板, 非科创板".format(date_str))
        return result["total"]

    #各时间段涨停数
    def time_up_count(self, date_str, time = "9:30"):
        result = self.ask("{} {}涨停，上市天数大于100，非退市，非st，非创业板, 非科创板".format(date_str, time))
        return result["total"]

    #昨日跌停竞价表现
    def prev_down_data(self, date_str):
        result = self.ask("{}涨幅且竞价涨幅且最大涨幅且前一日跌停，上市天数大于100，流通市值小于80亿，非退市，非st，非创业板, 非科创板".format(date_str))
        down_posi_count = 0
        down_nega_count = 0
        if result["total"] < 1:
            return [[0, 0], [0, 0]]
        for stock in result["result"]:
            if float(stock[self.open_pct_col])<0:#负溢价
                down_nega_count += 1
            else:#正溢价
                down_posi_count += 1
        return [[down_posi_count, round(down_posi_count/(down_posi_count + down_nega_count), 2)], [down_nega_count, round(down_nega_count/(down_posi_count + down_nega_count), 2)]]

    #昨日涨停表现
    def prev_up_data(self, date_str):
        result = self.ask("{}涨幅且竞价涨幅且最大涨幅且前一日涨幅且连板数且实际换手且首次涨停时间排序且几天几板，上市天数大于100，流通市值小于80亿，非退市，非st，非创业板, 非科创板".format(date_str))
        up_posi_count = 0
        cons_up_posi_count = 0
        firs_up_posi_count = 0
        up_nega_count = 0
        cons_up_nega_count = 0
        firs_up_nega_count = 0
        first_up_data = []
        for stock in result["result"]:
            try:
                stock[self.open_pct_col] = float(stock[self.open_pct_col])
                stock[self.close_pct_col][0] = float(stock[self.close_pct_col][0])
                stock[self.close_pct_col][1] = float(stock[self.close_pct_col][1])
                stock[self.high_pct_col] = float(stock[self.high_pct_col])
            except:
                stock[self.open_pct_col] = 0
                stock[self.close_pct_col][0] = 0
                stock[self.close_pct_col][1] = 0
                stock[self.high_pct_col] = 0
            stock[self.extra_col] = stock[self.close_pct_col][0]
            # if stock[self.open_pct_col] < -5:
            #     stock[self.extra_col] = min((stock[self.open_pct_col]  + stock[self.high_pct_col])/2, 0)
            # elif stock[self.open_pct_col] < 0:
            #     stock[self.extra_col] = (stock[self.open_pct_col]  + stock[self.high_pct_col])/2
            last_day_pct = (10-stock[self.close_pct_col][1]) if stock[self.close_pct_col][1] > 0 else stock[self.close_pct_col][1] - 10
            stock[self.extra_col] = stock[self.extra_col] - last_day_pct
            #负溢价
            if stock[self.open_pct_col] < 0:
                up_nega_count = up_nega_count + 1
                #连板
                if int(stock[self.cons_up_col]) > 1:
                    cons_up_nega_count += 1
                else:
                    firs_up_nega_count += 1
                    first_up_data.append(stock)
            else:
                up_posi_count +=  1
                if int(stock[self.cons_up_col]) > 1:
                    cons_up_posi_count += 1
                else:
                    firs_up_posi_count += 1
                    first_up_data.append(stock)
        return [[
                (up_posi_count,  round(up_posi_count/(up_posi_count + up_nega_count), 2) if up_posi_count + up_nega_count > 0 else 1), #涨停正溢价
                #(cons_up_posi_count, round(cons_up_posi_count/(cons_up_posi_count + cons_up_nega_count), 2)), #连板正溢价
                (firs_up_posi_count, round(firs_up_posi_count/(firs_up_posi_count + firs_up_nega_count), 2) if firs_up_posi_count + firs_up_nega_count > 0 else 1), #首板正溢价
            ], [
                (up_nega_count,  round(up_nega_count/(up_posi_count + up_nega_count), 2)) if up_posi_count + up_nega_count > 0 else 1, #涨停负溢价
                #(cons_up_nega_count, round(cons_up_nega_count/(cons_up_posi_count + cons_up_nega_count), 2)), #连板负溢价
                (firs_up_nega_count, round(firs_up_nega_count/(firs_up_posi_count + firs_up_nega_count), 2) if firs_up_posi_count + firs_up_nega_count > 0 else 1), #首板负溢价
            ],
            first_up_data, #首板数据
            result["indexID"], #表头
        ]

    #查询日期范围内的问句
    def loop_ask(self, start_date_str, end_date_str):
        s = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
        recent_ten = s-datetime.timedelta(days=10)
        prev_d = self.trade_date_list(recent_ten.strftime("%Y-%m-%d"), s-datetime.timedelta(days=1))
        dates = self.trade_date_list(prev_d[-1].strftime("%Y-%m-%d"), end_date_str)
        prev_trade_d = dates[0]
        del dates[0]
        first_up_posi_data = []
        first_up_nega_data = []
        pd_list = []
        total_pct = 1
        i_count = 0
        m_count = 0
        war_count = 0
        for d in dates:
            date_str = d.strftime("%Y-%m-%d")
            #空仓的条件
            # if i_count > 0:
            #     if (prev_up_data[0][0][1] > 0.7 and prev_up_counts[2] > 50):
            #         prev_up_data = self.prev_up_data(date_str)
            #         prev_up_counts = [self.time_up_count(prev_date_str, "9:30"), self.time_up_count(prev_date_str, "9:35"), self.time_up_count(prev_date_str, "")]
            #         war_count += 1
            #         if war_count == 2:
            #             war_count = 0
            #             continue

            #昨日日期
            prev_date_str = prev_trade_d.strftime("%Y-%m-%d")
            #昨日涨停表现
            prev_up_data = self.prev_up_data(date_str)
            #昨日首板数据
            first_up_data = prev_up_data[2]
            #表头
            title = prev_up_data[3]
            title[4] = str(title[4])
            del title[2], title[2], title[10],title[10],title[10],title[10],title[10],title[10],title[10],title[10],title[10],title[10]
            #prev_down_data = self.prev_down_data(date_str)
            #昨日涨停数
            prev_up_counts = [self.time_up_count(prev_date_str, "9:30"), self.time_up_count(prev_date_str, "9:35"), self.time_up_count(prev_date_str, "")]
            #隔行
            empty_row = ["" for x in range(0, 10)]
            pd_list.append(empty_row)
            #总收益
            open_pct_total = 1
            close_pct_total = 1
            real_pct_total = 1
            i_count += 1
            #当日实际模拟打板数量
            j_count = 0
            for stock in first_up_data:
                #放弃打板的条件
                if stock[self.open_pct_col] == stock[self.close_pct_col]:
                    continue
                first_up_time = datetime.datetime.fromtimestamp(float(stock[self.first_up_time_col])/1000)
                if first_up_time < datetime.datetime.strptime(first_up_time.strftime("%Y-%m-%d") + " 09:31", "%Y-%m-%d %H:%M"):
                    continue
                if first_up_time > datetime.datetime.strptime(first_up_time.strftime("%Y-%m-%d") + " 09:35", "%Y-%m-%d %H:%M"):
                    continue
                if stock[self.days_up_count] != "首板涨停" and stock[self.days_up_count] != "--":
                     continue
                if j_count == 4:
                    continue

                turn = int(float(stock[self.turn_col]))
                prev_down_count = self.down_count(prev_date_str)
                stock[self.turn_col] = str(turn) + "%"
                stock[self.first_up_time_col] = datetime.datetime.fromtimestamp(float(stock[self.first_up_time_col])/1000).strftime("%H:%M")
                #stock[self.final_up_time_col] = datetime.datetime.fromtimestamp(float(stock[self.final_up_time_col])/1000).strftime("%H:%M")
                empty_row[0] = " ".join([
                    "\n竞价涨停数:" + str(self.time_up_count(date_str, "9:30")),
                    "竞价跌停数:" + str(self.down_count(date_str)),
                    "\n昨日涨停数:" + str(prev_up_counts),
                    #"昨日跌停数:" + str(prev_down_count),
                    "\n昨日涨停正溢价数,比率:" + str((prev_up_data[0][0][0], prev_up_data[0][0][1])),
                    # "昨日涨停负溢价数,比率:" + str(prev_up_data[1]),
                    #"昨日跌停正溢价数,比率:" + str(prev_down_data[0]),
                    #"昨日跌停负溢价数,比率:" + str(prev_down_data[1]),
                ])
                open_pct_total += stock[self.open_pct_col]
                close_pct_total += stock[self.close_pct_col][0]
                real_pct_total += stock[self.extra_col]
                del stock[2], stock[2], stock[10],stock[10],stock[10],stock[10],stock[10],stock[10],stock[10],stock[10],stock[10],stock[10]
                pd_list.append(stock)
                j_count += 1
                m_count += 1
            if j_count > 0:
                empty_row[0] = date_str + " 平均收益:竞价" + str(round(open_pct_total/j_count, 2)) + "%, 收盘" + str(round(close_pct_total/j_count, 2)) + "%, 实际" + str(round(real_pct_total/j_count, 2)) + "%" + empty_row[0]
                total_pct *= (1 + real_pct_total/j_count/100)
            prev_trade_d = d


        empty_row = ["" for x in range(0, 10)]
        empty_row[0] = "累积收益(实际):" + str(round(total_pct*100 - 100, 2)) + "%" + " 平均每天持仓数:" + str(round(m_count/i_count, 2))
        print(empty_row[0])
        pd_list.append(empty_row)
        dataFrame = pd.DataFrame(pd_list, columns=title)
        with pd.ExcelWriter('./data/first_up_stock.xlsx') as writer:
            dataFrame.to_excel(writer, float_format='%.6f')

if __name__ == '__main__':
    try:
        core = Wencai()
        core.loop_ask("2022-3-1", "2022-6-30")
    except Exception as e:
        raise e
    finally:
        say("运行结束")
