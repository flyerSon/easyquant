# -*- coding: utf-8 -*-

import easyquotation
import time
import json
import sys
import logging
import os
from datetime import datetime,timedelta

#数据库
import pymysql
HOST = "39.98.151.63"
PORT = 3306
USER = "root"
PASSWD = "123456"
DB = "python_stock"
CHARSET = "utf8"
TABLE = "t_stock"
DATE = "f_"
LOG_FILE = None
con = None
logger = None
WIN_NUM = 3 
LOSE_NUM = 5
HISTORY_MAP = {}
INTEREST_LIST = None

#默认只分析10天的数据
DAY_NUM = 10
#分析日志只输出价格小于20的
MAX_PRICE = 20

'''
import threading
threadLock = threading.Lock()
'''


def connect_mysql():
    global con
    con = pymysql.connect(host=HOST,port=PORT,user=USER,passwd=PASSWD,db=DB,charset=CHARSET)

def close_mysql():
    global con
    if con:
        con.close()


##########################################################################处理数据开始###############################################################
def check_stock_table(table):
    ret = False
    global con
    cur = con.cursor()
    sql = "select count(*) from information_schema.columns where table_name = '{0}'".format(table)
    cur.execute(sql)
    ret_data = cur.fetchone()
    if ret_data[0] > 0:
        ret = True
    cur.close()
    return ret
    
def create_stock_table(table):
    if check_stock_table(table):
        logger.debug("[create_stock_table] {0} 表已经存在".format(table))
        return
    global con
    cur = con.cursor()
    sql = """create table if not exists {0}(
                f_id varchar(20) NOT NULL DEFAULT "" COMMENT "股票编号",
                f_name varchar(20) NOT NULL DEFAULT "" COMMENT "公司名字",
                PRIMARY KEY (f_id)
    ) ENGINE=MyISAM CHARSET=utf8 COMMENT="股票数据表"
    """.format(table)
    cur.execute(sql)
    cur.close()
    logger.debug("[create_stock_table] {0} 表创建成功".format(table))

def check_date_column(table,date):
    ret = False
    global con
    cur = con.cursor()
    sql = "select count(*) from information_schema.columns where table_name = '{0}' and column_name = '{1}'".format(table,date)
    cur.execute(sql)
    ret_data = cur.fetchone()
    if ret_data[0] > 0:
        ret = True
    cur.close()
    return ret
    
def add_date_column(table,date):
    if check_date_column(table,date):
        logger.debug("[add_date_column] {0} 列已经存在".format(date))
        return
    global con
    cur = con.cursor()
    sql = "alter table {0} add column {1} text(100000) NOT NULL COMMENT '日期'".format(table,date)
    cur.execute(sql)
    cur.close()
    logger.debug("[add_date_column] {0} 列创建成功".format(date))

def check_id_data(table,id):
    ret = False
    global con
    cur = con.cursor()
    sql = "select * from {0} where f_id = '{1}'".format(table,id)
    if cur.execute(sql) > 0:
        ret = True
    cur.close()
    return ret

def get_id_date_data(table,date,id):
    global con
    cur = con.cursor()
    sql = "select {0} from {1} where f_id = '{2}'".format(date,table,id)
    cur.execute(sql)
    ret_data = cur.fetchone()
    cur.close()
    return ret_data[0]

def get_same_day_all_dict_data(data_str):
    ret_vec = []
    data_str_vec = data_str.split("?")
    for data in data_str_vec:
        if data:
            ret_vec.append(eval(data))
    return ret_vec
        
#暂时只取最后一个值
def get_same_day_last_data(data_str):
    ret_vec = get_same_day_all_dict_data(data_str)
    last_data = None
    if len(ret_vec) > 0:
        last_data = ret_vec[len(ret_vec) - 1]
    return last_data
        
def insert_or_update_id_data(table,date,id,name,dict_data):
    global con
    data = str(dict_data)
    if not check_id_data(table,id):
        cur = con.cursor()
        sql = 'insert into {0} (f_id, f_name,{1})values("{2}","{3}","{4}")'.format(table,date,id,name,data)
        try:
            if cur.execute(sql) == 1:
                #logger.debug("[insert_or_update_id_data] 插入成功 {0},{1}".format(name,dict_data["date"]))
                print("[insert_or_update_id_data] 插入成功 {0},{1}".format(name,dict_data["date"]))
        except:
            print("[insert_or_update_id_data] 插入失败 {0},{1},{2}".format(name,dict_data["date"],sql))
        cur.close()
    else:
        add_flag = False
        
        old_data = get_id_date_data(table,date,id)
        last_data = get_same_day_last_data(old_data)
        if last_data:
            if last_data["time"] != dict_data["time"]:
                add_flag = True
        else:
            add_flag = True
        if add_flag:
            cur_data = old_data + "?" + data
            cur = con.cursor()
            sql = 'update {0} set {1} = "{2}" where f_id = "{3}"'.format(table,date,cur_data,id)
            if cur.execute(sql) == 1:
                #logger.debug("[insert_or_update_id_data] 更新成功 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
                print("[insert_or_update_id_data] 更新成功 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
            cur.close()
        else:
            #logger.debug("[insert_or_update_id_data] 数据重复，不用更新 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
            print("[insert_or_update_id_data] 数据重复，不用更新 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
  

  
#处理当天所有的股票数据
def main_do_date_today():
    quotation = easyquotation.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    data_vec = quotation.market_snapshot(prefix=True)
    check_time,table_flag,date_flag,table,date,date_vec = False,False,False,None,None,None
    localtime = time.localtime(time.time())
    num = 0
    for id in data_vec:
        stock_vec = data_vec[id]
        if not date_vec:
            date = stock_vec["date"]
            date_vec = date.split("-")
        #检查时间
        if not check_time:
            year,mon,day = int(date_vec[0]),int(date_vec[1]),int(date_vec[2])
            if year == localtime.tm_year:
                if mon == localtime.tm_mon:
                    if day != localtime.tm_mday:
                        #logger.debug("[do_date_today] 今天还没有数据 {0} {1} ".format(day,localtime.tm_mday))
                        break
            check_time = True
        if not table:
            table = TABLE + "_" + date_vec[0]
        if not table_flag:
            create_stock_table(table)
            table_flag = True
        if not date_flag:
            date = DATE + date_vec[1] + "_" + date_vec[2]
            add_date_column(table,date)
            date_flag = True
        insert_or_update_id_data(table,date,id,stock_vec["name"],stock_vec)
        num = num + 1
            
    logger.debug("[do_date_today] {0} {1} 数据已处理完毕".format(date,num))

##########################################################################处理数据结束###############################################################



##########################################################################分析数据开始###############################################################

#判断一天的走势，返回，当天是否涨(True),下午的开价
def compare_same_data(data_str,less_price_vec,max_price_vec):
    ret_vec = get_same_day_all_dict_data(data_str)
    #收集最低和最高
    for data in ret_vec:
        if less_price_vec[0] == 0:
            less_price_vec[0] = data["now"]
            less_price_vec[1] = data["date"]
        else:
            if less_price_vec[0] >  data["now"]:
                less_price_vec[0] = data["now"]
                less_price_vec[1] = data["date"]
        if max_price_vec[0] == 0:
            max_price_vec[0] = data["now"]
            max_price_vec[1] = data["date"]
        else:
            if max_price_vec[0] <  data["now"]:
                max_price_vec[0] = data["now"]
                max_price_vec[1] = data["date"]
    begin_end_vec = None
    if len(ret_vec) > 0:
        begin_end_vec = [False,ret_vec[len(ret_vec)-1]]
        #现价大于今日开盘价和大于昨日收盘价
        if begin_end_vec[1]["now"] > begin_end_vec[1]["close"]:
            begin_end_vec[0] = True
        '''
        if begin_end_vec[1]["now"] > begin_end_vec[1]["open"] and begin_end_vec[1]["now"] > begin_end_vec[1]["close"]:
            begin_end_vec[0] = True
        '''
    return begin_end_vec


#加载历史数据
def do_load_history_price():
    global HISTORY_MAP
    if HISTORY_MAP is None:
        out_file = "./history_log/" + "history.txt"
        fp = open(out_file,"r")
        line = fp.readline()
        while line:
            i = 0
            unit_vec = line.split(",")
            name,id,less_price_vec,max_price_vec = None,None,None,None
            for unit in unit_vec:
                vec = unit.split(":")
                if i == 0:
                    name = vec[1]
                elif i == 1:
                    id = vec[1]
                elif i == 2:
                    less_price_vec = [float(vec[1])]
                elif i == 3:
                    less_price_vec.append(vec[1])
                elif i == 4:
                    max_price_vec = [float(vec[1])]
                elif i == 5:
                    max_price_vec.append(vec[1])
                    HISTORY_MAP[id] = [less_price_vec,max_price_vec,name]
            i = i + 1
            line = fp.readline()
        fp.close()
        logger.debug("[do_load_history_price] 加载历史数据 {0}".format(len(HISTORY_MAP)))

#生成历史数据
def do_write_history_price(ret_map):
    localtime = time.localtime(time.time())
    date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
    out_file = "./history_log/" + date + "_history.txt"
    fp = open(out_file,"w")
    for key in ret_map:
        condition_vec = ret_map[key]
        less_price_vec,max_price_vec,id = None,None,None
        if len(condition_vec) > 1:
            less_price_vec = condition_vec[1]
        if len(condition_vec) > 2:
            max_price_vec = condition_vec[2]
        if len(condition_vec) > 4:
            id = condition_vec[4]
        if less_price_vec and max_price_vec and id:
            history_price = "股票名字:{0},股票id:{1},最低价:{2},日期:{3},最高价:{4},日期:{5}\n".format(key,id,less_price_vec[0],less_price_vec[1],max_price_vec[0],max_price_vec[1])
            fp.write(history_price)
    fp.close()
    price_file = "./history_log/" + "history.txt"
    command = 'cp -rf {0} {1}'.format(out_file,price_file) 
    os.system(command)
    logger.debug("[do_write_history_price] 生成历史数据 {0}".format(len(ret_map)))

'''
#现价大于昨日收盘价才算涨
def do_analysis_one_stock(table,id,ret_map):
    global con
    cur = con.cursor()
    while True:
        sql = "select * from {0} where f_id = '{1}'".format(table,id)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        continue_num,last_win_flag,ret_vec = 0,None,None
        less_price_vec,max_price_vec,now_price_vec = [0,0],[0,0],[0,0]
        if id in HISTORY_MAP:
            unit = HISTORY_MAP[id]
            less_price_vec = unit[0]
            max_price_vec = unit[1]
        for data_vec in ret_data:
            begin_pos = 2
            if len(data_vec) > DAY_NUM:
                begin_pos = len(data_vec) - DAY_NUM
                if begin_pos < 2:
                    begin_pos = 2
            for i in range(begin_pos,len(data_vec)):
                day_begin_end_vec = compare_same_data(data_vec[i],less_price_vec,max_price_vec)
                if not day_begin_end_vec:
                    continue
                day_data = day_begin_end_vec[1]
                if not day_data["name"] in ret_map:
                    condition_vec = [[]] 
                    ret_map[day_data["name"]] = condition_vec
                condition_vec = ret_map[day_data["name"]]
                if ret_vec is None:
                    ret_vec = condition_vec[0] 
                up_flag = day_begin_end_vec[0]
                if last_win_flag is None:
                    #开始涨或者跌的日期,上日收盘价,连涨次数,结束日期,结束现价
                    vec = [up_flag,day_data["date"],day_data["close"],1,day_data["date"],day_data["now"]]
                    ret_vec.append(vec)
                else:
                    last_vec = ret_vec[len(ret_vec) - 1]
                    #记住最后一次涨跌结束的时间
                    if up_flag != last_win_flag:
                        vec = [up_flag,day_data["date"],day_data["close"],1,day_data["date"],day_data["now"]]
                        ret_vec.append(vec)
                    else:
                        #连涨或者连跌
                        last_vec[3] = last_vec[3] + 1
                        last_vec[4] = day_data["date"]
                        last_vec[5] = day_data["now"]
                last_win_flag = up_flag
        if len(data_vec) > 2:
                day_begin_end_vec = compare_same_data(data_vec[len(data_vec) - 1],less_price_vec,max_price_vec)
                day_data = day_begin_end_vec[1]
                now_price_vec[0] = day_data["now"]
                now_price_vec[1] = day_data["date"]
             
        condition_vec.append(less_price_vec)
        condition_vec.append(max_price_vec)
        condition_vec.append(now_price_vec)
        condition_vec.append(id)
        break
    cur.close()
'''




#现价大于昨日收盘价才算涨
def do_analysis_one_stock(table,id,ret_map):
    global con
    cur = con.cursor()
    while True:
        #非跨表
        sql,change_table_flag = get_select_sql(table,id,False)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        #跨表
        if change_table_flag:
            sql,_ = get_select_sql(table,id,True)
            if cur.execute(sql) <= 0:
                break
            ret_data = cur.fetchall() + ret_data
        continue_num,last_win_flag,ret_vec = 0,None,None
        less_price_vec,max_price_vec,now_price_vec = [0,0],[0,0],[0,0]
        if id in HISTORY_MAP:
            unit = HISTORY_MAP[id]
            less_price_vec = unit[0]
            max_price_vec = unit[1]
        for data_vec in ret_data:
            #range是前闭后开集合
            for i in range(0,len(data_vec)):
                day_begin_end_vec = compare_same_data(data_vec[i],less_price_vec,max_price_vec)
                if not day_begin_end_vec:
                    continue
                day_data = day_begin_end_vec[1]
                if not day_data["name"] in ret_map:
                    condition_vec = [[]] 
                    ret_map[day_data["name"]] = condition_vec
                condition_vec = ret_map[day_data["name"]]
                if ret_vec is None:
                    ret_vec = condition_vec[0] 
                up_flag = day_begin_end_vec[0]
                if last_win_flag is None:
                    #开始涨或者跌的日期,上日收盘价,连涨次数,结束日期,结束现价
                    vec = [up_flag,day_data["date"],day_data["close"],1,day_data["date"],day_data["now"]]
                    ret_vec.append(vec)
                else:
                    last_vec = ret_vec[len(ret_vec) - 1]
                    #记住最后一次涨跌结束的时间
                    if up_flag != last_win_flag:
                        vec = [up_flag,day_data["date"],day_data["close"],1,day_data["date"],day_data["now"]]
                        ret_vec.append(vec)
                    else:
                        #连涨或者连跌
                        last_vec[3] = last_vec[3] + 1
                        last_vec[4] = day_data["date"]
                        last_vec[5] = day_data["now"]
                last_win_flag = up_flag
        if len(data_vec) > 2:
                day_begin_end_vec = compare_same_data(data_vec[len(data_vec) - 1],less_price_vec,max_price_vec)
                if day_begin_end_vec is not None:
                    day_data = day_begin_end_vec[1]
                    now_price_vec[0] = day_data["now"]
                    now_price_vec[1] = day_data["date"]
             
        condition_vec.append(less_price_vec)
        condition_vec.append(max_price_vec)
        condition_vec.append(now_price_vec)
        condition_vec.append(id)
        break
    cur.close()


'''
#多线程分析数据    
def main_do_analysis_data_thread():
    localtime = time.localtime(time.time())
    table = TABLE + "_" + str(localtime.tm_year)
    global con
    cur = con.cursor()
    ret_map,thread_ret_map = {},{}
    num = 0
    while True:
        logger.debug("[do_analysis_data] 分析数据开始")
        sql = "select f_id from {0}".format(table)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        thread_do_analysis(table,ret_data,thread_ret_map)
        while True:
            complete_flag = True 
            threadLock.acquire()
            for key,ret_vec in thread_ret_map.items():
                if not ret_vec[0]:
                    complete_flag = False
                    break
            threadLock.release()
            if not complete_flag:
                time.sleep(2)
            else:
                for key,ret_vec in thread_ret_map.items():
                    num = num + len(ret_vec[1])
                    ret_map.update(ret_vec[1])
                break
        break
    cur.close()
    do_log_analysis(ret_map)

def do_analysis_one_stock_thread(table,id,ret_map):
    global con
    cur = con.cursor()
    while True:
        threadLock.acquire()
        sql = "select * from {0} where f_id = '{1}'".format(table,id)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        threadLock.release()
        continue_num,last_win_flag = 0,None
        less_price_vec = [0,0]
        max_price_vec = [0,0]
        now_price_vec = [0,0]
        for data_vec in ret_data:
            win_vec,lose_vec = None,None
            for i in range(2,len(data_vec)):
                day_begin_end_vec = compare_same_data(data_vec[i],less_price_vec,max_price_vec)
                if not day_begin_end_vec:
                    continue
                day_data = day_begin_end_vec[1]
                if not day_data["name"] in ret_map:
                    condition_vec = [[],[]] 
                    ret_map[day_data["name"]] = condition_vec
                condition_vec = ret_map[day_data["name"]]
                #如果上次跟这次涨跌情形不一致，则置0
                if day_begin_end_vec[0] != last_win_flag:
                    continue_num = 0
                continue_num = continue_num + 1
                if win_vec is None:
                    win_vec = condition_vec[0]
                if lose_vec is None:
                    lose_vec = condition_vec[1]
                if continue_num == 1:
                    if day_begin_end_vec[0]:
                        #开始涨的日期，连涨次数
                        vec = [day_data["date"],continue_num,day_data["date"]]
                        win_vec.append(vec)
                    else:
                        #开始跌的日期，连跌次数
                        vec = [day_data["date"],continue_num,day_data["date"]]
                        lose_vec.append(vec)
                    if not last_win_flag is None:
                        if day_begin_end_vec[0]:
                            vec = lose_vec[len(lose_vec)-1]
                            vec[2] = day_data["date"]
                        else:
                            vec = win_vec[len(win_vec)-1]
                            vec[2] = day_data["date"]
                    last_win_flag = day_begin_end_vec[0]
                else:
                    if day_begin_end_vec[0]:
                        vec = win_vec[len(win_vec)-1]
                        vec[1] = vec[1] + 1
                        vec[2] = day_data["date"]
                    else:
                        vec = lose_vec[len(lose_vec)-1]
                        vec[1] = vec[1] + 1
                        vec[2] = day_data["date"]

        if len(data_vec) > 2:
                day_begin_end_vec = compare_same_data(data_vec[len(data_vec) - 1],less_price_vec,max_price_vec)
                if day_begin_end_vec is not None:
                    day_data = day_begin_end_vec[1]
                    now_price_vec[0] = day_data["now"]
                    now_price_vec[1] = day_data["date"]
             
        condition_vec.append(less_price_vec)
        condition_vec.append(max_price_vec)
        condition_vec.append(now_price_vec)
        break
    cur.close()

def thread_do_analysis_job(table,thread_id,job_list,thread_ret_map):
    ret_vec = [False,{}]
    ret_map = ret_vec[1]
    for id_vec in job_list:
        do_analysis_one_stock_thread(table,id_vec[0],ret_map)
    ret_vec[0] = True
    threadLock.acquire()
    thread_ret_map[thread_id] = ret_vec 
    threadLock.release()
         

def thread_do_analysis(table,ret_data,thread_ret_map):
    thread_id,job,thread_job = 1,0,[] 
    thread_job = []
    for i in range(len(ret_data)):
        thread_job.append(ret_data[i])
        job = job + 1
        if job >= 10:
            break
        if job == 1000:
            job_list = list(thread_job)
            threadLock.acquire()
            thread_ret_map[thread_id] = [False,{}]
            threadLock.release()
            th = threading.Thread(target=thread_do_analysis_job,args=(table,thread_id,job_list,thread_ret_map,))
            th.start()
            job = 0
            thread_job = []
            thread_id = thread_id + 1
    if len(thread_job) > 0:
        threadLock.acquire()
        thread_ret_map[thread_id] = [False,{}]
        threadLock.release()
        th = threading.Thread(target=thread_do_analysis_job,args=(table,thread_id,thread_job,thread_ret_map,))
        th.start()

'''
#拼凑输出字符串
def get_log_format_change(arg_map):
    log_ret = ("[do_analysis_data] {0}{1} 从 {2} {3} 开始连续{4} {5} 次 到 {6} {7} 结束".format(arg_map["prefix"],arg_map["name"],arg_map["begin_date"],arg_map["begin"],arg_map["up"],arg_map["cnt"],arg_map["end_date"],arg_map["end"]))
    return log_ret


#拼凑输出字符串
def get_log_format(arg_map):
    log_ret = ("[do_analysis_data] {0}{1} 从 {2} 开始连续{3} {4} 次 到 {5} 结束 最低 {6} {7} 最高 {8} {9} 当前 {10}".format(arg_map["prefix"],arg_map["name"],arg_map["begin"],arg_map["up"],arg_map["cnt"],arg_map["end"],arg_map["less"],arg_map["less_date"],arg_map["max"],arg_map["max_date"],arg_map["now"]))
    return log_ret


#输出分析结果到日志
def do_log_analysis(ret_map):
    for key in ret_map:
        condition_vec = ret_map[key]
        vec = condition_vec[0]
        less_price_vec,max_price_vec,now_price_vec = [0,0],[0,0],[0,0]
        if len(condition_vec) > 1:
            less_price_vec = condition_vec[1]
        if len(condition_vec) > 2:
            max_price_vec = condition_vec[2]
        if len(condition_vec) > 3:
            now_price_vec = condition_vec[3]
        if now_price_vec[0] >= MAX_PRICE:
            continue
        logger.debug("[do_analysis_data] {0} 分析开始 最低 {1} {2} 最高 {3} {4} 当前 {5}".format(key,less_price_vec[0],less_price_vec[1],max_price_vec[0],max_price_vec[1],now_price_vec[0]))
        for i in range(0,len(vec)):
            unit = vec[i]
            last_tag = "" 
            if i == len(vec) - 1:
                last_tag = "最后" 
        #for unit in vec:
            #筛选掉价格不变的
            if less_price_vec[0] == max_price_vec[0]:
                continue
            cnt = unit[3]
            arg_map = {}
            arg_map["name"] = key
            arg_map["prefix"] = ""
            arg_map["up"] = ""
            arg_map["begin_date"] = unit[1] 
            arg_map["end_date"] = unit[4] 
            arg_map["begin"] = unit[2] 
            arg_map["end"] = unit[5] 
            arg_map["cnt"] = cnt 
            arg_map["less"] = less_price_vec[0] 
            arg_map["less_date"] = less_price_vec[1]
            arg_map["max"] = max_price_vec[0] 
            arg_map["max_date"] = max_price_vec[1] 
            arg_map["now"] = now_price_vec[0] 
            log_level = 0
            if unit[0]:
                arg_map["up"] = last_tag + "涨"
                if cnt == WIN_NUM - 1:
                    log_level = 1
                    arg_map["prefix"] = "稍微注意连涨 "
                elif cnt == WIN_NUM:
                    arg_map["prefix"] = "关注注意连涨 "
                    log_level = 2
                elif cnt > WIN_NUM:
                    arg_map["prefix"] = "特别注意连涨 "
                    log_level = 3
            else:
                arg_map["up"] = last_tag + "跌"
                if cnt >= LOSE_NUM - 2 and cnt <= LOSE_NUM - 1:
                    arg_map["prefix"] = "稍微注意连跌 "
                    log_level = 1
                elif cnt == LOSE_NUM:
                    arg_map["prefix"] = "关注注意连跌 "
                    log_level = 2
                elif cnt > LOSE_NUM:
                    arg_map["prefix"] = "特别注意连跌 "
                    log_level = 3
            log_ret = get_log_format_change(arg_map)
            if log_level == 0:
                logger.debug(log_ret)
            elif log_level == 1:
                logger.info(log_ret)
            elif log_level == 2:
                logger.warning(log_ret)
            else:
                logger.error(log_ret)
        logger.debug("[do_analysis_data] {0} 分析结束".format(key))

#分析数据    
def main_do_analysis_data():
    #加载历史数据
    do_load_history_price()

    localtime = time.localtime(time.time())
    table = TABLE + "_" + str(localtime.tm_year)
    global con
    cur = con.cursor()
    ret_map = {}
    num = 0
    while True:
        logger.debug("[do_analysis_data] 分析数据开始")
        sql = "select f_id from {0}".format(table)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        for id_vec in ret_data:
            do_analysis_one_stock(table,id_vec[0],ret_map)
            num = num + 1
        break
    cur.close()
    do_log_analysis(ret_map)
    do_write_history_price(ret_map)
    logger.debug("[do_analysis_data] 分析 {0} 数据结束".format(num))

##########################################################################分析数据结束###############################################################


##########################################################################剔除重复数据开始#############################################################

def tick_id_date_repeat_data(table,date,id):
    global con
    cur = con.cursor()
    repeat_flag = False
    while True:
        if not check_id_data(table,id):
            break
        data_str = get_id_date_data(table,date,id)
        ret_vec = get_same_day_all_dict_data(data_str)
        if len(ret_vec) == 0:
            break
        last_data,last_string = None,None
        for each_data in ret_vec:
            if not last_data or last_data["time"] != each_data["time"]:
                last_data = each_data
                if not last_string:
                    last_string = str(last_data)
                else:
                    last_string = last_string + "?" + str(last_data)
            else:
                if last_data["time"] == each_data["time"]:
                    repeat_flag = True
        if not repeat_flag:
            break
        sql = 'update {0} set {1} = "{2}" where f_id = "{3}"'.format(table,date,last_string,id)
        if cur.execute(sql) == 1:
            logger.debug("[tick_id_date_repeat_data] 剔除重复数据成功 {0},{1}".format(id,date))
        break
    cur.close()
    if not repeat_flag:
        logger.debug("[tick_id_date_repeat_data] 没有剔除的重复数据 {0},{1}".format(id,date))

def get_all_column_name(table):
    global con
    cur = con.cursor()
    column_vec = []
    while True:
        sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '{0}' and table_schema = '{1}'".format(table,DB)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        for data_vec in ret_data:
            column_vec.append(data_vec[0])
        break
    cur.close()
    return column_vec
        
#剔除重复数据        
def main_tick_repeat_data():
    localtime = time.localtime(time.time())
    table = TABLE + "_" + str(localtime.tm_year)
    global con
    cur = con.cursor()
    ret_map = {}
    num = 0
    while True:
        logger.debug("[tick_repeat_data] 剔除重复数据开始")
        sql = "select f_id from {0}".format(table)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        column_vec = get_all_column_name(table)
        if len(column_vec) == 0:
            break
        for id_vec in ret_data:
            for i in range(2,len(column_vec)):
                tick_id_date_repeat_data(table,column_vec[i],id_vec[0])
            num = num + 1
        break
    cur.close()
    logger.debug("[tick_repeat_data] 剔除 {0} 重复数据结束".format(num))

##########################################################################剔除重复数据结束#############################################################



def init_log():
    global logger
    localtime = time.localtime(time.time())
    date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)

    global LOG_FILE
    if LOG_FILE is None:
        LOG_FILE = "./history_log/" + date + ".log"
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s-%(levelname)s-%(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)


#初始化兴趣列表
def init_interest_list():
    global INTEREST_LIST 
    if INTEREST_LIST is None:
        INTEREST_LIST = ["世纪华通","巨人网络","东睦股份","恺英网络","游族网络","光线传媒","菲利华"]

#抓取对应的日志
def do_interest(name,out):
    command = 'cat {0} | grep {1} >> {2}'.format(LOG_FILE,name,out)
    os.system(command)

#抓取对应的日志
def do_attention(attention_level):
    name = None
    if attention_level == 1:
        name = "关注注意连涨"
    elif attention_level == 2:
        name = "特别注意连涨"
    elif attention_level == 3:
        name = "关注注意连跌"
    elif attention_level == 4:
        name = "特别注意连跌"
    else:
        print("[do_attention] 参数错误 {0} ".format(attention_level))
    if not name is None:
        localtime = time.localtime(time.time())
        date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
        out = "./history_log/" + date + "_{0}.log".format(name)
        do_interest(name,out)
        print("[do_attention] 提取股票完成 {0} ".format(name))

def main_do_interest():
    init_interest_list()
    localtime = time.localtime(time.time())
    date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
    out = "./history_log/" + date + "_interest.log"
    for name in INTEREST_LIST:
        do_interest(name,out)
    print("[main_do_interest] 提取兴趣股票完成 {0} 只".format(len(INTEREST_LIST)))

def get_select_sql(now_table,id,change_table_flag):
    ret = False
    sql = "select "  
    today = datetime.now()
    size,i,column_vec,table = 0,0,[],None
    while size < DAY_NUM and i < 3 * DAY_NUM:
        day = datetime.strftime(datetime.now() - timedelta(i), '%Y_%m_%d')
        vec = day.split("_")
        if len(vec) >= 3:
            column = DATE + vec[1] + "_" + vec[2] 
            table = TABLE + "_" + vec[0]
            if not change_table_flag:
                if table == now_table:
                    if check_date_column(table,column):
                        column_vec.append(column)
                        size = size + 1
                else:
                    ret = True
            else:
                if table != now_table:
                    if check_date_column(table,column):
                        column_vec.append(column)
                        size = size + 1
        i = i + 1
    last_table = now_table 
    if change_table_flag:
        last_table = table
    for i in range(0,len(column_vec)):
        if i >= 1:
            sql = sql + ","
        sql = sql + column_vec[len(column_vec) - i - 1] 
    sql = sql + " from {0} where f_id = '{1}'".format(last_table,id)
    return sql,ret
                
if __name__ == "__main__":
    begin_time = time.time()
    while True:
        if len(sys.argv) < 2:
            print("[main] 参数数量不正确 {0} 提示 collect,analysis,tick,interest,attention".format(sys.argv))
            break
        connect_mysql()    
        init_log()
        arg = sys.argv[1]
        logger.debug("[main] 开始处理指令 {0}".format(arg))
        if arg == "collect":
            main_do_date_today()
        elif arg == "analysis":
            main_do_analysis_data()
        elif arg == "analysis_week":
            DAY_NUM = 7
            main_do_analysis_data()
        elif arg == "analysis_month":
            DAY_NUM = 30
            main_do_analysis_data()
        elif arg == "tick":
            main_tick_repeat_data()
        elif arg == "interest":
            main_do_interest()
        elif arg == "attention":
            level = int(sys.argv[2])
            if level == 0:
                for i in range(1,5):
                    do_attention(i)
            else:
                do_attention(int(level))
        else:
            print("[main] 参数错误 {0} 提示 collect,analysis,tick,interest,attention".format(sys.argv))
        close_mysql()
        break
    logger.debug("[main] 处理指令 {0} 花费时间 {1} 秒".format(arg,time.time()-begin_time))
