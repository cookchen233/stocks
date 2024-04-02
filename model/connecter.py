# -*- coding: utf-8 -*-

import os, time
from tool import *
import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,BigInteger, DECIMAL, Integer,SmallInteger, String, Text, ForeignKey, Date, DateTime, UniqueConstraint, Index, sql, text

class Connecter(object):

    connections = {}

    @staticmethod
    def connect(connect_link):
        engine = sqlalchemy.create_engine(
            connect_link,
            max_overflow=10,  # 超过连接池大小外最多创建的连接
            pool_size=10,  # 连接池大小
            pool_timeout=60,  # 池中没有线程最多等待的时间，否则报错
            pool_recycle=-1,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
            echo=False
        )
        #此种方式可保证线程安全, 每次提交后使用 session.remove()回收
        SessionFactory = sessionmaker(bind=engine)
        session = scoped_session(SessionFactory)

        #此种方式在多线程下, 需要为每个线程取得一个连接 SessionFactory(), 使用session.close()回收
        #SessionFactory = sessionmaker(bind=engine)
        #session = SessionFactory()

        # session._model_changes = {}

        return [session, engine]

    @staticmethod
    def get_instance(connect_link):
        connect_link_md5 = md5(connect_link)
        if not connect_link_md5 in Connecter.connections:
            Connecter.connections[connect_link_md5] = Connecter.connect(connect_link)
        return Connecter.connections[connect_link_md5]

class DataBase(object):

    # Child class to be set
    connect_link = ''

    session = object
    engine = object

    def __init__(self):
        self.connect_link = "mysql+pymysql://root:@127.0.0.1:3306/stocks?charset=utf8"
        connection = Connecter.get_instance(self.connect_link)
        self.session = connection[0]
        self.engine = connection[1]

    def __enter__(self):
        return self

    def  __exit__(self, exc_type, exc_val, exc_tb):
        self.session.remove()

Base = declarative_base()

class StockDaily(Base):
    __tablename__ = 'stock_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    name = Column(String)
    type = Column(Integer, default=0)
    industry = Column(String, default="")
    date = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    amplitude = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    volume_ratio = Column(DECIMAL, default=1)
    turnover = Column(DECIMAL)
    is_limit_up = Column(Integer, default=0)
    keep_limit_up_days = Column(Integer, default=0)
    free_shares = Column(Integer)
    market_value = Column(BigInteger)
    pre_close = Column(DECIMAL, default=0)
    pre_pct_chg = Column(DECIMAL, default=0)
    pre_turnover = Column(DECIMAL, default=0)
    avp5 = Column(DECIMAL, default=0)
    avp20 = Column(DECIMAL, default=0)
    avp30 = Column(DECIMAL, default=0)
    avp60 = Column(DECIMAL, default=0)
    avp120 = Column(DECIMAL, default=0)
    avp20_chg5 = Column(DECIMAL, default=0)
    avp60_chg5 = Column(DECIMAL, default=0)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")

class IndexDaily(Base):

    __tablename__ = 'index_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    name = Column(String)
    type = Column(Integer, default=0)
    follow = Column(Integer, default=0)
    date = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    amplitude = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    volume_ratio = Column(DECIMAL, default=1)
    turnover = Column(DECIMAL)
    free_shares = Column(Integer)
    market_value = Column(BigInteger)
    members = Column(String)
    top_members = Column(String)
    pre_close = Column(DECIMAL, default=0)
    pre_pct_chg = Column(DECIMAL, default=0)
    pre_turnover = Column(DECIMAL, default=0)
    avp5 = Column(DECIMAL, default=0)
    avp20 = Column(DECIMAL, default=0)
    avp30 = Column(DECIMAL, default=0)
    avp60 = Column(DECIMAL, default=0)
    avp120 = Column(DECIMAL, default=0)
    avp20_chg5 = Column(DECIMAL, default=0)
    avp60_chg5 = Column(DECIMAL, default=0)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")

class EtfDaily(Base):

    __tablename__ = 'etf_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    name = Column(String)
    type = Column(Integer, default=0)
    follow = Column(Integer, default=0)
    date = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    amplitude = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    volume_ratio = Column(DECIMAL, default=1)
    turnover = Column(DECIMAL)
    free_shares = Column(Integer)
    market_value = Column(BigInteger)
    members = Column(String)
    top_members = Column(String)
    pre_close = Column(DECIMAL, default=0)
    pre_pct_chg = Column(DECIMAL, default=0)
    pre_turnover = Column(DECIMAL, default=0)
    avp5 = Column(DECIMAL, default=0)
    avp20 = Column(DECIMAL, default=0)
    avp30 = Column(DECIMAL, default=0)
    avp60 = Column(DECIMAL, default=0)
    avp120 = Column(DECIMAL, default=0)
    avp20_chg5 = Column(DECIMAL, default=0)
    avp60_chg5 = Column(DECIMAL, default=0)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")

class StockGroup(Base):

    __tablename__ = 'stock_group'

    id = Column(Integer, primary_key=True, autoincrement=True)
    create_time = Column(String(255), nullable=False)
    group_code = Column(String(255), nullable=False)
    code = Column(String(255), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    percent = Column(DECIMAL, nullable=True)

class SsfStock(Base):

    __tablename__ = 'ssf_stock'
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(50), nullable=False)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    shares = Column(BigInteger, nullable=False)
    equity_pct = Column(DECIMAL, nullable=False)
    amount = Column(DECIMAL, nullable=False)
    change_shares = Column(BigInteger, nullable=False)
    change_amount = Column(DECIMAL, nullable=False)
    change_pct = Column(DECIMAL, nullable=False)
    position_pct = Column(DECIMAL, nullable=True)
    stock_inc_rate = Column(DECIMAL, nullable=False)
    income = Column(DECIMAL, nullable=False)
    cum_income = Column(DECIMAL, nullable=False)

class StockFiveMinutes(Base):
    __tablename__ = 'stock_five_minutes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    name = Column(String)
    date = Column(String)
    time = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    day_pct_chg = Column(DECIMAL)
    day_pct_chg2 = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")

class StockM5Klines(Base):
    __tablename__ = 'stock_m5_klines'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True)
    name = Column(String)
    date = Column(String)
    time = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    day_pct_chg = Column(DECIMAL)
    day_pct_chg2 = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")


class MinuteKlines(Base):
    __tablename__ = 'minute_klines'
    id = Column(Integer, primary_key=True, autoincrement=True)
    create_time = Column(String)
    code = Column(String)
    name = Column(String)
    trans_time = Column(String)
    open = Column(DECIMAL)
    high = Column(DECIMAL)
    low = Column(DECIMAL)
    close = Column(DECIMAL)
    pct_chg = Column(DECIMAL)
    day_pct_chg = Column(DECIMAL)
    day_pct_chg2 = Column(DECIMAL)
    volume = Column(BigInteger)
    amount = Column(BigInteger)
    turnover = Column(DECIMAL)
    real_turnover = Column(DECIMAL)
    field_m = Column(Integer, default=0)
    field_n = Column(Integer, default=0)
    field_i = Column(DECIMAL, default=0)
    field_j = Column(DECIMAL, default=0)
    field_x = Column(String, default="")
    field_y = Column(String, default="")

