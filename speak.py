import sys
import pyttsx3

def init_engine():
    engine = pyttsx3.init()
    engine.setProperty('rate', 300)
    return engine

def say(s):
    engine.say(s)
    engine.runAndWait() #blocks

engine = init_engine()
say(str(sys.argv[1]))