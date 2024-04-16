#使用方法

git clone https://github.com/cookchen233/stocks.git

cd stocks

# 监控封单(涨停,跌停), 成交量激增, 高换手
python3 monitor.py up 0
# 非交易时间段(用于调试)
python3 monitor.py up 1

# 实时获取前8日2连板以上个股的行情强弱(需每日更新conf/risk.txt)
python3 crawl_kline.py live

# 绘制前8日2连板以上个股的强弱图表(根据前面crawl_kline.py获取的数据生成)
python3 market_heat.py




