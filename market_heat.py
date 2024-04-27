import sys
from subprocess import call

from tool import *
import pandas as pd
from model.connecter import *

from datetime import datetime, timedelta, time
from time import sleep
import stock
import matplotlib.pyplot as plt
from matplotlib import MatplotlibDeprecationWarning
import warnings
import numpy as np

plt.rcParams["font.family"] = "Arial Unicode MS"
warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)


class Heat(object):
    stock = None
    db = None

    interval = 5
    limit_up_range_days = 8
    limit_up_days = 2

    # 龙头周期配置
    leadings = [
        ("川大", datetime(2024, 4, 24), datetime(2024, 4, 29)),
        ("中衡", datetime(2024, 4, 17), datetime(2024, 4, 25)),
        ("春光", datetime(2024, 4, 12), datetime(2024, 4, 21)),
        ("建设", datetime(2024, 4, 10), datetime(2024, 4, 15)),
        ("莱绅", datetime(2024, 4, 8), datetime(2024, 4, 12)),
        ("联明", datetime(2024, 4, 3), datetime(2024, 4, 7)),
        ("华生", datetime(2024, 3, 21), datetime(2024, 4, 2)),
        ("宁科", datetime(2024, 3, 20), datetime(2024, 3, 28)),
        ("艾艾", datetime(2024, 3, 5), datetime(2024, 3, 21)),
        ("安彩", datetime(2024, 3, 4), datetime(2024, 3, 11)),
        ("安奈", datetime(2024, 2, 27), datetime(2024, 3, 6)),
        ("东方", datetime(2024, 2, 23), datetime(2024, 3, 1)),
        ("克莱", datetime(2024, 2, 1), datetime(2024, 2, 27)),
        ("中视", datetime(2024, 1, 23), datetime(2024, 2, 1)),
        ("深中", datetime(2024, 1, 9), datetime(2024, 1, 25)),
        ("长白", datetime(2024, 1, 2), datetime(2024, 1, 17)),
        ("亚世", datetime(2023, 12, 25), datetime(2024, 1, 2)),
        ("南京", datetime(2023, 11, 28), datetime(2023, 12, 12)),
        ("东安", datetime(2023, 11, 23), datetime(2023, 12, 5)),
        ("三柏", datetime(2023, 11, 10), datetime(2023, 11, 22)),
        ("银宝", datetime(2023, 11, 10), datetime(2023, 11, 21)),
        ("皇庭", datetime(2023, 11, 7), datetime(2023, 11, 15)),
        ("天威", datetime(2023, 11, 2), datetime(2023, 11, 10)),
        ("天龙", datetime(2023, 10, 25), datetime(2023, 11, 7)),
        ("高新", datetime(2023, 10, 19), datetime(2023, 11, 2)),
        ("龙洲", datetime(2023, 10, 20), datetime(2023, 10, 30)),
        ("真视", datetime(2023, 10, 16), datetime(2023, 10, 26)),
        ("圣龙", datetime(2023, 9, 28), datetime(2023, 10, 25)),
        ("精伦", datetime(2023, 9, 9), datetime(2023, 9, 20)),
        ("捷荣", datetime(2023, 8, 29), datetime(2023, 9, 27)),
        ("我乐", datetime(2023, 8, 28), datetime(2023, 9, 6)),
        ("金科", datetime(2023, 7, 21), datetime(2023, 7, 31)),
        ("大连", datetime(2023, 7, 5), datetime(2023, 7, 13)),
        ("鸿博", datetime(2023, 5, 25), datetime(2023, 6, 7)),
        ("睿能", datetime(2023, 5, 19), datetime(2023, 5, 29)),
        ("日播", datetime(2023, 4, 25), datetime(2023, 5, 26)),
        ("剑桥", datetime(2023, 3, 20), datetime(2023, 4, 26)),
        ("汉王", datetime(2023, 1, 7), datetime(2023, 2, 7)),
        ("麦趣", datetime(2022, 12, 21), datetime(2022, 12, 30)),
        ("格力", datetime(2022, 12, 9), datetime(2022, 12, 19)),
        ("人人", datetime(2022, 12, 6), datetime(2022, 12, 14)),
        ("通润", datetime(2022, 11, 16), datetime(2022, 12, 6)),
        ("安奈", datetime(2022, 11, 24), datetime(2022, 12, 7)),
        ("科传", datetime(2022, 11, 22), datetime(2022, 11, 30)),
        ("天鹅", datetime(2022, 10, 31), datetime(2022, 11, 21)),
        ("国脉", datetime(2022, 10, 10), datetime(2022, 10, 19)),
        ("襄阳", datetime(2022, 7, 22), datetime(2022, 8, 1)),
        ("亚联", datetime(2022, 7, 11), datetime(2022, 7, 26)),
        ("恒大", datetime(2022, 7, 11), datetime(2022, 7, 19)),
        ("山西", datetime(2022, 7, 6), datetime(2022, 7, 14)),
        ("金智", datetime(2022, 7, 4), datetime(2022, 7, 12)),
        ("赣能", datetime(2022, 6, 28), datetime(2022, 7, 8)),
        ("传艺", datetime(2022, 6, 23), datetime(2022, 7, 4)),
        ("松芝", datetime(2022, 6, 17), datetime(2022, 7, 29)),
        ("集泰", datetime(2022, 6, 10), datetime(2022, 6, 28)),
        ("海汽", datetime(2022, 5, 30), datetime(2022, 6, 14)),
        ("华西", datetime(2022, 6, 1), datetime(2022, 6, 10)),
        ("特力", datetime(2022, 5, 24), datetime(2022, 6, 2)),
        ("中通", datetime(2022, 5, 13), datetime(2022, 5, 31)),
        ("索菱", datetime(2022, 5, 11), datetime(2022, 5, 20)),
        ("新华", datetime(2022, 4, 26), datetime(2022, 5, 13)),
        ("建艺", datetime(2022, 4, 27), datetime(2022, 5, 12)),
        ("浙江", datetime(2022, 4, 26), datetime(2022, 5, 10)),
        ("湖南", datetime(2022, 4, 25), datetime(2022, 5, 11)),
        ("中交", datetime(2022, 3, 23), datetime(2022, 4, 11)),
        ("天保", datetime(2022, 3, 16), datetime(2022, 3, 31)),
        ("北坡", datetime(2022, 3, 15), datetime(2022, 3, 25)),
        ("盘龙", datetime(2022, 3, 16), datetime(2022, 3, 28)),
        ("中医", datetime(2022, 3, 2), datetime(2022, 3, 21)),
        ("准油", datetime(2022, 2, 24), datetime(2022, 3, 4)),
        ("美丽", datetime(2022, 2, 18), datetime(2022, 2, 28)),
        ("诚邦", datetime(2022, 2, 14), datetime(2022, 2, 22)),
        ("浙江", datetime(2022, 2, 7), datetime(2022, 2, 21)),
        ("保利", datetime(2022, 1, 26), datetime(2022, 2, 11)),
        ("得利", datetime(2022, 1, 12), datetime(2022, 1, 20)),
        ("翠微", datetime(2022, 1, 4), datetime(2022, 1, 20)),
        ("开开", datetime(2021, 12, 31), datetime(2022, 1, 12)),
        ("顾地", datetime(2021, 12, 29), datetime(2022, 1, 7)),
    ]

    def __init__(self):
        self.stock = stock.Stock()
        self.db = DataBase()

    def get_x_comment(self, txt):
        date_str = txt[0:10]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        result = "混沌", "blue"
        for i in range(len(self.leadings)):
            comment, range_start, range_end = self.leadings[i]
            if date == after_dates(range_end, 1)[0]:
                result = comment, "green"
                break
            if range_start <= date <= range_end:
                result = comment, "red"
            if date > range_end:
                break
        return result

    def draw(self, x_data, y_data):
        fig, ax = plt.subplots(figsize=(36, 9))  # 设置图形的宽度和高度

        ax.plot(x_data, y_data)

        ax.set_ylabel('温度', fontsize=24)
        ax.set_title('A股短线交易市场情绪指数', fontsize=28)

        ax.tick_params(axis='y', labelsize=20)
        # y轴字体精细化设置
        # for yt in ax.get_yticklabels():
        #     val = int(yt.get_text())
        #     yt.set_fontsize(14)

        # 绘制灰色虚线作为分割线
        for idx, x_val in enumerate(x_data):
            if '15:00' in x_val:
                ax.axvline(idx, color=(0.8, 0.8, 0.8), linestyle='--')

        x_ticks = []
        x_ticklabels = []
        for idx, label in enumerate(ax.get_xticklabels()):
            txt = label.get_text()
            label.set_text(txt[11:16])
            if '09:30' in txt or '10:30' in txt or '11:30' in txt or '14:00' in txt or '14:30' in txt:
                if '09:30' in txt:
                    label.set_fontsize(22)
                    label.set_text(txt[0:10])
                elif '14:00' in txt:
                    label.set_horizontalalignment('right')
                    x_comment, x_color = self.get_x_comment(txt)
                    ax.text(label.get_position()[0], label.get_position()[1] + 20, x_comment, fontsize=26,
                            color=x_color, horizontalalignment='right')
                else:
                    label.set_fontsize(12)
            else:
                label.set_visible(False)
            x_ticks.append(idx)
            x_ticklabels.append(label)

        ax.set_xticks(x_ticks)
        ax.set_xticklabels(x_ticklabels)
        # x轴刻度文字倾斜角度
        plt.xticks(rotation=70)
        # x轴刻度与两端的边距
        plt.margins(x=0.005)
        plt.tight_layout()

        # 在整个图形上均匀分布水印
        watermark_text = "济南估下 Watermark"
        num_watermarks = 6  # 水印总数

        # 定义水印均匀分布的位置
        positions = [
            (0.17, 0.7), (0.5, 0.7), (0.83, 0.7),  # 上行
            (0.17, 0.2), (0.5, 0.2), (0.83, 0.2)  # 下行
        ]
        txts = [
            "金岸股侠",
            "抖音45722015770",
        ]

        # 在预设的每个位置放置水印
        i = 0
        for pos_x, pos_y in positions:
            i = 1 if i == 0 else i - 1
            ax.text(pos_x, pos_y, txts[i], transform=ax.transAxes, fontsize=36, color='gray', rotation=30,
                    ha='center', va='center', alpha=0.3)

        filename = "./data/market-heat-{}-to-{}-{}-{}.png".format(x_data[-1][0:10], x_data[0][:10], self.limit_up_days,
                                                                  self.limit_up_range_days)
        plt.savefig(filename)

    def generate_interval_minutes(self):
        start_time_morning = datetime.strptime("09:30", "%H:%M")
        end_time_morning = datetime.strptime("11:30", "%H:%M")

        start_time_afternoon = datetime.strptime("13:" + str(self.interval).zfill(2), "%H:%M")
        end_time_afternoon = datetime.strptime("15:00", "%H:%M")

        time_data = []

        current_time = start_time_morning
        while current_time <= end_time_morning:
            time_data.append(current_time.strftime("%H:%M"))
            current_time += timedelta(minutes=self.interval)

        current_time = start_time_afternoon
        while current_time <= end_time_afternoon:
            time_data.append(current_time.strftime("%H:%M"))
            current_time += timedelta(minutes=self.interval)

        time_data.append(current_time.strftime("%H:%M"))
        return time_data

    def get_xy_data(self, date):
        minutes = self.generate_interval_minutes()
        date_str = date.strftime("%Y-%m-%d")
        x_data = []
        y_data = []
        for minute in minutes:
            if date.day == datetime.now().day:
                if datetime.strptime(date_str + " " + minute, "%Y-%m-%d %H:%M") >= datetime.now():
                    continue
            if "15:" + str(self.interval).zfill(2) in minute:
                x_value, y_value = date_str + " " + minute, np.nan
            else:
                x_value, y_value = self.calculate_xy(date, minute)
            x_data.append(x_value)
            y_data.append(y_value)
        return x_data, y_data

    def calculate_xy(self, date, minute):
        x_value = date.strftime("%Y-%m-%d") + " " + minute
        # klines = self.db.session.query(MinuteKlines).filter(*[
        #     MinuteKlines.trans_time == x_value + ":00",
        # ]).all()
        stocks = self.stock.get_continuous_limit_up_stocks(date, self.limit_up_days, self.limit_up_range_days)
        codes = [stock.code for stock in stocks]  # 提取每个结果的 code 字段并组装成数组
        query = (
            self.db.session.query(MinuteKlines)
            .filter(
                MinuteKlines.code.in_(codes),
                MinuteKlines.close_time == datetime.strptime(date.strftime("%Y-%m-%d") + " " + minute, "%Y-%m-%d %H:%M")
            )
        )
        klines = query.all()
        sql = query.statement.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        # print(sql)
        pct_chg = 0
        coef_high = 1
        coef_low = 1
        count = len(klines)
        for kline in klines:
            # 涨跌幅系数, 当日涨跌幅与相对开盘的涨跌幅的均值.
            avg = (kline.day_pct_chg + kline.day_pct_chg2) / 2
            if kline.day_pct_chg2 > 7:
                coef_high *= 1.01
            elif kline.day_pct_chg2 < -7:
                if kline.day_pct_chg2 < -12:
                    coef_low *= (1 - 0.01 * 4)
                else:
                    coef_low *= (1 - 0.02 * 2)
            pct_chg += float(avg)

        coef_high = max(coef_high, 0)

        # 极端情况下, 所有股票天地板将是 -15%, +15用于抹去y轴的负数表示
        coef_pct_chg = (pct_chg / count) + 15
        coef_count = (1 + count / 200)

        print(date.date(), minute, len(stocks), coef_count, coef_pct_chg, coef_high, coef_low)
        y_value = coef_pct_chg * coef_count * coef_high * coef_low
        return x_value, y_value


if __name__ == '__main__':
    heat = Heat()
    # dates = before_dates(datetime.strptime("2024-04-16", "%Y-%m-%d"), 20)
    dates = before_dates(datetime.now(), 20)
    today = dates.pop()
    x_data = []
    y_data = []
    # 往期数据, 始终保持
    for date in dates:
        x, y = heat.get_xy_data(date)
        x_data.extend(x)  # 使用 extend 方法将新的数据合并到列表中
        y_data.extend(y)

    # 当天数据, 追加
    # heat.interval = 2
    while True:
        try:
            cur_min = int(datetime.now().strftime("%M"))
            if cur_min % 5 != 0 or not heat.stock.right_time("09:30", "12:00") and not heat.stock.right_time("12:00", "15:30"):
                print("等待")
                sleep(1)
                continue
            x, y = heat.get_xy_data(today)
            if len(x) == 0:
                print("无当日数据")
                sleep(1)
                continue

            x2, y2 = x_data[:], y_data[:]
            x2.extend(x)
            y2.extend(y)
            heat.draw(x2, y2)
            print("生存成功")
        except Exception as e:
            err_log()
            sleep(60)
            call(["python3", os.path.abspath(os.path.dirname(__file__)) + "/speak.py", "生成绪数指数发生错误"])
