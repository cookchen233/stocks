# coding=utf-8
import pickle
import re,os,json,sys,oss2,hashlib,requests,random,datetime, time,uuid,urllib,math,psutil,random,decimal, glob
import shelve
import subprocess

import inspect
import traceback

import hashlib
import hmac
import base64

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

def say(msg):
    subprocess.call(["python3", os.path.abspath(os.path.dirname(__file__)) + "/speak.py", msg])

def retry_decorator(func, sleep_time = 1):
    def wrapper(*arg, **kw):
        def try_func(try_times = 1):
            try:
                return func(*arg, **kw)
            except Exception as e:
                if try_times >= 3:
                    raise e
                time.sleep(sleep_time)
                return try_func(try_times + 1)
        return try_func()
    return wrapper

def to_decimal(value):
    value = str(value)
    if value.replace("-", "a").isalpha():
        return decimal.Decimal("0")
    return decimal.Decimal(value)

def hex_str(str):
    hexs=''
    for v in str.decode():
        hexs += '\\u'+v.encode('hex')
    return hexs

def fdate(format='%Y-%m-%d %H:%M:%S'):
    time_now = int(time.time())
    time_local = time.localtime(time_now)
    return time.strftime(format, time_local)

def tuuid(return_num=False):
    #时刻变化
    id = str(decimal.Decimal(time.time())).replace('.', '')[1:25]
    if return_num:
        return id;
    return radix(id, 36, 10)

def radix(number, to=62, fro=10):
    #先转换成10进制
    number = dec_from(number, fro)
    #再转换成目标进制
    number = dec_to(number, to)
    return number

def dec_to(num, to = 62):
    num = int(num)
    if to == 10 or to > 62 or to < 2:
        return num
    dict = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    if to > 36:
        dict = '0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ'
    ret = ''
    while num >= 1:
        ret = dict[int(num % to)] + ret
        num = num / to
    return ret

def dec_from(num, fro = 62):
    if fro == 10 or fro > 62 or fro < 2:
        return num
    num = str(num)
    dict = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    if fro > 36:
        dict = '0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ'
    len = len(num)
    dec = 0
    for i in range(0, len):
        pos = dict.find(num[i]);
        if pos >= fro:
            continue; #如果出现非法字符，会忽略掉。比如16进制中出现w、x、y、z等
    dec = math.pow(fro, len - i - 1) * pos + dec
    return dec

def mkfile(filename, content):
    paths = filename.split('/')
    path = ''
    for i in range(len(paths) - 1):
        path += paths[i] + '/'
        if os.path.exists(path) is False:
            os.makedirs(path)
    with open(filename, 'a') as f:
        f.write(content)

def log(name, *args):
    path = os.path.dirname(os.path.abspath(__file__)) + "/log/" +'/'+ name + '/'
    filename = path + fdate('%Y-%m-%d') +'.log'
    last_filename = path + "last.log"
    #print(os.path.dirname(os.path.abspath(__file__)) + "/log/" +'/'+ name + '/' + fdate('%Y-%m-%d') +'.log')
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
    args = list(args)
    for index, arg in enumerate(args):
        if isinstance(args, object):
            args[index] = str(arg)
    content = "[" + fdate() + "] " + json.dumps(args, indent=4, ensure_ascii=False) + "\n\n"
    if not os.path.isfile(filename) and os.path.isfile(last_filename):
        with open(last_filename, 'r+') as f:
            f.seek(0)
            f.truncate()
    mkfile(last_filename, content)
    mkfile(filename , content)
    #pr(json.dumps(args, indent=4, ensure_ascii=False))

def err_log(*args):
    ex_type, ex_msg, ex_tb = sys.exc_info()
    caller = traceback.extract_stack()[-2:-1][0]
    # log_name = caller[0][caller[0].rfind('/')+1:] + str(caller[1])
    log_name = "error"
    ex_f_tb = ()
    for v in traceback.format_tb(ex_tb):
        ex_f_tb = ex_f_tb + (v.strip().replace('\n', ''),)
    info = (log_name,) + ex_f_tb + (str(ex_type), str(ex_msg)) + args
    log(*info)
    return info

def cache(prefix, key, value = False):
    key = str(key)
    ring = HashRing(range(0, 100))
    dirs = [ring.get_node(md5(key, i)) for i in range(1, 3)]
    dirs.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/cache/" + prefix)
    dirs.append(md5(key))
    filename = '{}/{}/{}/{}.pickle'.format(*tuple(dirs))

    if value is False:
    #     with shelve.open(filename) as db:
    #         for k, v in db.items():
    #             if k == key:
    #                 return v
        if not os.path.isfile(filename):
            return None
        with open(filename, 'rb') as f: # 读入时同样需要指定为读取字节流模式
            # The protocol version used is detected automatically, so we do not
            # have to specify it.
            return pickle.load(f)
    else:
        # with shelve.open(filename) as db:
        #     db[key] = value
        #     return True
        path = os.path.dirname(filename)
        if not os.path.isdir(path):
            os.makedirs(path, exist_ok=True)
        with open(filename, 'wb') as f: # 此处需指定模式为写入字节流，因为pickle是将object转换为二进制字节流
            # Pickle the 'data' dictionary using the highest protocol available.
            return pickle.dump(value, f, pickle.HIGHEST_PROTOCOL) # 采用最新的协议，扩展性较好

def cache_list(prefix, key, append_value = "get_list_cache"):
    list = cache(prefix, key)
    if not list:
        list = []
    if append_value != "get_list_cache":
        list.append(append_value)
        cache(prefix, key, list)
    return list

def pr(*args):
    print(*((fdate(),)  + args), flush=True)

def dump(data):
    return json.dumps(data, indent=4, ensure_ascii=False)

def load_header_string(header_string):
    arr = header_string.split('\n')
    headers = {}
    for v in arr:
        search = re.search(r'^[\s\n]*?((?!\:)\S+?)\:[\s]*?(.*?)$', v)
        if(search):
            headers[search.group(1)] = search.group(2).strip()
    return headers


def sub_popen(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.wait()
    return proc.stdout.read().decode().strip()

def listdir_nohidden(path):
    return glob.glob(os.path.join(path, '*'))

def md5(src, stacking = 1):
    m = hashlib.md5()
    m.update(src.encode('utf-8'))
    ret = m.hexdigest()
    for i in range(1, max(1, int(stacking))):
        ret = md5(ret)
    return ret

def ok(*args):
    pr("\033[1;32m" + " ".join(map(lambda x: str(x), args)) + "\033[1;0m")


def sendmail(title, content, receivers = []):
    """
    The arguments are:
        - from_addr    : The address sending this mail.
        - receivers     : A tuple list of addresses to send this mail to. tuple 0 is nickname and 1 is address.
                            such as {"Wayne":"waynechen@hainabian.com", "Kent":"kentzhang@hainabian.com"}
        - title          : The mail subject.
        - content : The mail Content
    """
    if not receivers:
        for v in os.getenv("err_receiver").split(","):
            receiver = v.split(":")
            receivers.append((receiver[0].strip(), receiver[1].strip()))
    sender = os.getenv("mail_user")
    msg = MIMEText(content,'html','utf-8')
    msg['Subject'] = title
    msg['From'] = "Python Script Error<{}>".format(sender)
    msg['To'] = ",".join([formataddr((addr[0], addr[1])) for addr in receivers])
    smtp = smtplib.SMTP(os.getenv("mail_host"))
    smtp.login(sender, os.getenv("mail_pass"))
    smtp.sendmail(sender, [addr[1] for addr in receivers], msg.as_string())
    smtp.quit()

class HashRing:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """
        Adds a `node` to the hash ring (including a number of replicas)
        """
        for i in range(self.replicas):
            virtual_node = f"{node}#{i}"
            key = self.gen_key(virtual_node)
            self.ring[key] = node
            self._sorted_keys.append(key)
            # print(f"{virtual_node} --> {key} --> {node}")

        self._sorted_keys.sort()
        # print([self.ring[key] for key in self._sorted_keys])

    def remove_node(self, node):
        """
        Removes `node` from the hash ring and its replicas
        """
        for i in range(self.replicas):
            key = self.gen_key(f"{node}#{i}")
            del self.ring[key]
            self._sorted_keys.remove(key)

    def get_node(self, string_key):
        """
        Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        return self.get_node_pos(string_key)[0]

    def get_node_pos(self, string_key):
        """
        Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None, None

        key = self.gen_key(string_key)
        nodes = self._sorted_keys
        for i in range(len(nodes)):
            node = nodes[i]
            if key < node:
                return self.ring[node], i

        # 如果key > node，那么让这些key落在第一个node上就形成了闭环
        return self.ring[nodes[0]], 0

    def gen_key(self, string_key):
        """
        Given a string key it returns a long value, this long value represents
        a place on the hash ring
        """
        m = hashlib.md5()
        m.update(string_key.encode('utf-8'))
        return m.hexdigest()

def array_chunk(list, rows=2, cols=2):
    arr = list[:]
    l=len(arr)
    if rows:
        rows = min(rows,l)
        ret=[[arr.pop(0) for y in range(0, math.ceil(l/rows)) if len(arr)] for x in range(0, rows)]
    else:
        cols = min(cols,l)
        ret=[[arr.pop(0) for y in range(0, cols) if len(arr)] for x in range(0, math.ceil(l/cols))]
    return ret

def get_part_filename(filename):
    return os.path.basename(os.path.dirname(filename)) + "/" + os.path.basename(filename)