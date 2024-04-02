import datetime

from sqlalchemy import create_engine, MetaData, Table, select, insert



from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from model.connecter import MinuteKlines, StockM5Klines

# 创建数据库引擎
engine = create_engine("mysql+pymysql://root:@127.0.0.1:3306/mydata?charset=utf8")

# 创建会话工厂
Session = sessionmaker(bind=engine)

# 声明基类
Base = declarative_base()

# 创建所有表格
Base.metadata.create_all(engine)

# 创建会话
session = Session()

# 查询旧表数据
old_data = session.query(StockM5Klines).all()

# 将旧表数据插入到新表
create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

engine2 = create_engine("mysql+pymysql://root:@127.0.0.1:3306/stocks?charset=utf8")

# 创建会话工厂
Session2 = sessionmaker(bind=engine2)

# 声明基类
Base = declarative_base()

# 创建所有表格
Base.metadata.create_all(engine)

# 创建会话
session2 = Session2()

for row in old_data:
    new_row = MinuteKlines(
        create_time=create_time,
        code=row.code,
        name=row.code,
        trans_time=row.time,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        pct_chg=row.pct_chg,
        day_pct_chg=row.day_pct_chg,
        day_pct_chg2=row.day_pct_chg2,
        volume=row.volume,
        amount=row.amount,
        turnover=0,
        real_turnover=0,
    )
    session2.add(new_row)

# 提交事务
session2.commit()

# 关闭会话
session2.close()
session.close()
