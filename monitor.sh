ps -ef | grep /Users/chen/maat/coding/python/stocks/monitor.py | grep -v "grep" | awk '{print $2}' | xargs kill -9
nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py etf 0 >/dev/null  2>&1  &
#nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py buying 0 >/dev/null  2>&1  &
nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py up 0 >/dev/null  2>&1  &
nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py weight 0 >/dev/null  2>&1  &
#nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py report 0 >/dev/null  2>&1  &
#nohup python3 /Users/chen/maat/coding/python/stocks/monitor.py bond 0 >/dev/null  2>&1  &
ps -ef |grep stocks/monitor