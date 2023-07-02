import baostock as bs
import pandas as pd
import math,datetime,requests,re,json,time,random,pandas,hashlib, sklearn, numpy
from bs4 import BeautifulSoup
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from decimal import *

from tool import *
from model.connecter import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor, as_completed

class BaoStockImport(object):

    baostock = None
    def __init__(self):
        pass

    def  __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_stock_info(self,  code, date = None):
        with DataBase() as db:
            query_list = [
                StockDaily.code == code,
            ]
            if date:
                query_list.append(StockDaily.date == date)
            info = db.session.query(StockDaily).filter(*query_list).first()
            return info
    def update_ssf_stocks(self):
        def upd(offset, limit):
            with DataBase() as db:
                ssf_stock_list = db.session.query(SsfStock).order_by(SsfStock.date.asc()).limit(limit).offset(offset).all()
                for i, row in enumerate(ssf_stock_list):
                    stock_info = db.session.query(StockDaily).filter(*[
                        StockDaily.code == row.code,
                        StockDaily.date < (row.date-datetime.timedelta(days=90))
                    ]).order_by(StockDaily.date.desc()).first()
                    stock_info2 = db.session.query(StockDaily).filter(*[
                        StockDaily.code == row.code,
                        StockDaily.date > (row.date-datetime.timedelta(days=90))
                    ]).order_by(StockDaily.date.asc()).first()
                    stock_info = stock_info if stock_info else stock_info2
                    if not stock_info:
                        print("ssf_stock为空", row.code, row.date-datetime.timedelta(days=90))
                        continue
                    invested_found = db.session.query(SsfStock).with_entities(func.sum(SsfStock.amount).label('invested_found')).filter(*[
                        SsfStock.date  ==  row.date
                    ]).scalar()
                    data = {}
                    data["amount"] = row.shares * stock_info.close
                    data["change_amount"] = row.change_shares * stock_info.close
                    data["position_pct"] = Decimal(row.amount)/Decimal(invested_found) * 100

                    cur_stock_info = db.session.query(StockDaily).filter(*[
                        StockDaily.code == row.code,
                        StockDaily.date < row.date
                    ]).order_by(StockDaily.date.desc()).first()
                    stock_inc_rate = (cur_stock_info.close - stock_info.close)/stock_info.close
                    data["stock_inc_rate"] = stock_inc_rate * 100
                    data["income"] = stock_inc_rate * data["amount"]
                    if data["income"] == 0:
                        print(stock_info.code, stock_inc_rate, data["amount"], cur_stock_info.date, stock_info.date, cur_stock_info.close,stock_info.close, (row.date-datetime.timedelta(days=90)), row.date )
                    data["cum_income"] = db.session.query(SsfStock).with_entities(func.sum(SsfStock.income)).filter(*[
                        SsfStock.code == row.code,
                        SsfStock.date <= row.date
                    ]).scalar()
                    db.session.query(SsfStock).filter(SsfStock.id == row.id).update(data)
                    db.session.commit()
                    # print ("ssf_stock成功更新", row.name)
            print("阶段完成", offset)
        with DataBase() as db:
            count = db.session.query(func.count(SsfStock.id)).scalar()
        limit = 100
        pool_count = math.ceil(count/limit)
        pool = ThreadPoolExecutor(pool_count)
        threads = []
        for i in range(pool_count):
            t = pool.submit(upd, i*limit, limit)
            threads.append(t)
        pool.shutdown()
        print("全部更新完成")

    def save_ssf_stocks_from_east_money(self):
        dates = [
            # "2016-12-31",
            # "2017-03-30",
            # "2017-06-30",
            # "2017-09-30",
            # "2017-12-31",
            # "2018-03-30",
            # "2018-06-30",
            # "2018-09-30",
            # "2018-12-31",
            # "2019-03-30",
            # "2019-06-30",
            # "2019-09-30",
            # "2019-12-31",
            # "2020-03-30",
            # "2020-06-30",
            # "2020-09-30",
            # "2020-12-31",
            "2021-03-31",
            "2021-06-30",
            "2021-09-30",
            "2021-12-31",
            "2022-03-31",
        ]
        for d in dates:
            with DataBase() as db:
                db.session.query(SsfStock).filter(SsfStock.date == d).delete()
            for i in range(1, 100):
                url = "https://data.eastmoney.com/dataapi/zlsj/list?date={}&type=3&zjc=0&sortField=HOULD_NUM&sortDirec=1&pageNum={}&pageSize=50&p={}&pageNo={}&pageNumber={}".format(d, i, i, i, i)
                r = requests.get(url)
                resp_data = json.loads(r.text)
                if not "data" in resp_data:
                    print("no data", url)
                    break
                with DataBase() as db:
                    for v in resp_data["data"]:
                        data = {
                            "market": v["SECUCODE"][-2:].lower(),
                            "code": v["SECURITY_CODE"],
                            "name": v["SECURITY_NAME_ABBR"],
                            "date": datetime.datetime.strptime(v["REPORT_DATE"], "%Y-%m-%d %H:%M:%S"),
                            "shares": int(v["TOTAL_SHARES"]),
                            "amount": 0.0,
                            "position_pct": 0.0,
                            "stock_inc_rate": 0.0,
                            "income": 0.0,
                            "cum_income": 0.0,
                            "equity_pct": Decimal(v["TOTALSHARES_RATIO"]),
                            "change_shares": int(v["HOLDCHA_NUM"]),
                            "change_amount": 0.0,
                            "change_pct": Decimal(v["HOLDCHA_RATIO"]) if v["HOLDCHA_RATIO"] is not None else 0,
                        }
                        stock_info = self.get_stock_info(data["code"], data["date"]-datetime.timedelta(days=90))
                        if stock_info:
                            data["amount"] = data["shares"] * stock_info.close
                            data["change_amount"] = data["change_shares"] * stock_info.close
                        query_list = [
                            SsfStock.date == data["date"],
                            SsfStock.code == data["code"],
                        ]
                        db_data = db.session.query(SsfStock).filter(*query_list).first()
                        if db_data:
                            print("ssf_stock跳过",data["name"])
                            continue
                        db.session.add(SsfStock(**data))
                        db.session.commit()
                    print("完成 ", url, len(resp_data["data"]))

    def get_ssf_stock_statistics(self, start_date):
        with DataBase() as db:
            # 季度列表
            date_list = db.session.query(SsfStock).with_entities(SsfStock.date).filter(*[SsfStock.date >= datetime.datetime.strptime(start_date, "%Y-%m-%d")]).group_by(SsfStock.date).order_by(SsfStock.date.asc()).all()
            date_list = [date.date.strftime("%Y-%m-%d") for date in date_list]
            #所有重仓股
            all_high_ssf_stock_list = []
            all_high_ssf_stock_statistic_list = []
            name_list = []
            for date in date_list:
                #该季度重仓/调仓比例大
                db_data = db.session.query(SsfStock).filter(*[SsfStock.date == date]).order_by(SsfStock.position_pct.desc(), SsfStock.change_pct.desc()).limit(40)
                for row in db_data:
                    all_high_ssf_stock_list.append(row.__dict__)
                db_data = db.session.query(SsfStock).filter(*[SsfStock.date == date]).order_by(SsfStock.change_pct.desc(), SsfStock.position_pct.desc()).limit(40)
                for row in db_data:
                    all_high_ssf_stock_list.append(row.__dict__)

            #仓位/调仓比例降序
            all_high_ssf_stock_list = sorted(all_high_ssf_stock_list, key = lambda i: (i['change_pct'], i['position_pct']), reverse=True)
            name_list = []
            for stock in all_high_ssf_stock_list:
                name = stock["code"] + " " + stock["name"]
                if name in name_list:
                    continue
                name_list.append(name)
                stock_date_info_list = []
                #查询该股票该季度数据
                for date in date_list:
                    stock_info = db.session.query(SsfStock).filter(*[SsfStock.date == date, SsfStock.code == stock["code"]]).first()
                    if stock_info:
                        flag = ""
                        if stock["id"] == stock_info.id:
                            flag = "↑↑↑"
                        type = ""
                        if stock_info.change_shares > 0:
                            type = "↑"
                        elif stock_info.change_shares < 0:
                            type  =  "↓"
                        stock_date_info_list.append("{}%, {}% {}, {}%, {}亿, {}亿".format(
                            round(stock_info.position_pct, 2),
                            round(stock_info.change_pct, 2),
                            type,
                            # flag,
                            round(stock_info.stock_inc_rate, 2),
                            round(stock_info.income/100000000, 2),
                            round(stock_info.cum_income/100000000, 2),
                        ))
                    else:
                        stock_date_info_list.append("")
                all_high_ssf_stock_statistic_list.append(stock_date_info_list)

        dataFrame = pd.DataFrame(all_high_ssf_stock_statistic_list, index=name_list, columns=date_list)

        with pd.ExcelWriter('./data/ssf.xlsx') as writer:
            dataFrame.to_excel(writer, float_format='%.6f')


if __name__ == '__main__':
    #BaoStockImport().save_ssf_stocks_from_east_money()
    #BaoStockImport().update_ssf_stocks()
    BaoStockImport().get_ssf_stock_statistics("2021-03-31")





