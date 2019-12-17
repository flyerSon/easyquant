# -*- coding: utf-8 -*-

import time
import json
import sys
import logging
import os

logger = None
interest_list = None
file_name = None
log_file = None


#初始化兴趣列表
def init_interest_list():
    global interest_list 
    interest_list = ["世纪华通","巨人网络","东睦股份","恺英网络","游族网络","光线传媒"]

#初始化日志
def init_log():
    global logger
    localtime = time.localtime(time.time())
    date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)
    global file_name
    file_name = "./history_log/" + date + ".log"
    global log_file 
    log_file = "./history_log/" + date + "_interest.log"
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s-%(levelname)s-%(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)


#抓取对应的日志
def do_interest(name):
    command = 'cat {0} | grep {1} >> {2}'.format(file_name,name,log_file)
    os.system(command)
    print("[do_interest] 提取兴趣股票完成 {0} ".format(name))

def main_do_interest():
    init_log()
    init_interest_list()
    for name in interest_list:
        do_interest(name)
    logger.debug("[main_do_interest] 提取兴趣列表完成 共 {0} 只".format(len(interest_list)))
     
if __name__ == "__main__":
    begin_time = time.time()
    while True:
        if len(sys.argv) < 2:
            print("[main] 参数数量不正确 {0} 提示 interest".format(sys.argv))
            break
        arg = sys.argv[1]
        if arg == "interest":
            main_do_interest()
        else:
            print("[main] 参数错误 {0} 提示 interest".format(sys.argv))
        break
    print("[main] 处理花费时间 {0} 秒".format(time.time()-begin_time))
