import sys
import pyttsx3


def init_engine():
    engine = pyttsx3.init()
    # 设置语速
    engine.setProperty('rate', 420)  # 设置语速为正常速度的410%
    # 设置音量
    engine.setProperty('volume', 1.0)  # 设置音量为最大

    return engine


def say(s):
    engine.say(s)
    engine.runAndWait()


engine = init_engine()
say(str(sys.argv[1]))
