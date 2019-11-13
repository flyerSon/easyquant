# -*- coding: utf-8 -*-

import easyquotation
import time
import json
import sys
import logging

#数据库
import pymysql
HOST = "121.40.77.217"
PORT = 3306
USER = "flyer"
PASSWD = "flyer"
DB = "python_stock"
CHARSET = "utf8"
TABLE = "t_stock"
DATE = "f_"
con = None
logger = None
WIN_NUM = 3 
LOSE_NUM = 5


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
        logger.info("[create_stock_table] {0} 表已经存在".format(table))
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
    logger.info("[create_stock_table] {0} 表创建成功".format(table))

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
        logger.info("[add_date_column] {0} 列已经存在".format(date))
        return
    global con
    cur = con.cursor()
    sql = "alter table {0} add column {1} text(100000) NOT NULL COMMENT '日期'".format(table,date)
    cur.execute(sql)
    cur.close()
    logger.info("[add_date_column] {0} 列创建成功".format(date))

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
        if cur.execute(sql) == 1:
            logger.info("[insert_or_update_id_data] 插入成功 {0},{1}".format(name,dict_data["date"]))
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
                logger.info("[insert_or_update_id_data] 更新成功 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
            cur.close()
        else:
            logger.info("[insert_or_update_id_data] 数据重复，不用更新 {0},{1} {2}".format(name,dict_data["date"],dict_data["time"]))
  
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
                        logger.info("[do_date_today] 今天还没有数据 {0} {1} ".format(day,localtime.tm_mday))
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
            
    logger.info("[do_date_today] {0} {1} 数据已处理完毕".format(date,num))

##########################################################################处理数据结束###############################################################



##########################################################################分析数据开始###############################################################

#判断一天的走势，返回，当天是否涨(True),下午的开价
def compare_same_data(data_str):
    ret_vec = get_same_day_all_dict_data(data_str)
    begin_end_vec = None
    if len(ret_vec) > 0:
        begin_end_vec = [False,ret_vec[len(ret_vec)-1]]
        #现价大于今日开盘价和大于昨日收盘价
        if begin_end_vec[1]["now"] > begin_end_vec[1]["open"] and begin_end_vec[1]["now"] > begin_end_vec[1]["close"]:
            begin_end_vec[0] = True
    return begin_end_vec
    
def do_analysis_one_stock(table,id,ret_map):
    global con
    cur = con.cursor()
    while True:
        sql = "select * from {0} where f_id = '{1}'".format(table,id)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        continue_num,last_win_flag = 0,False
        for data_vec in ret_data:
            for i in range(2,len(data_vec)):
                day_begin_end_vec = compare_same_data(data_vec[i])
                if not day_begin_end_vec:
                    continue
                day_data = day_begin_end_vec[1]
                if not day_data["name"] in ret_map:
                    condition_vec = [[],[]] 
                    ret_map[day_data["name"]] = condition_vec
                condition_vec = ret_map[day_data["name"]]
                #如果上次跟这次涨跌情形不一致，则置0
                if day_begin_end_vec[0] != last_win_flag:
                    last_win_flag = day_begin_end_vec[0]
                    continue_num = 0
                continue_num = continue_num + 1
                if last_win_flag:
                    win_vec = condition_vec[0]
                    if continue_num == 1:
                        #开始涨的日期，连涨次数
                        vec = [day_data["date"],continue_num]
                        win_vec.append(vec)
                    else:
                        vec = win_vec[len(win_vec)-1]
                        vec[1] = vec[1] + 1
                else:
                    lose_vec = condition_vec[1]
                    if continue_num == 1:
                        #开始跌的日期，连跌次数
                        vec = [day_data["date"],continue_num]
                        lose_vec.append(vec)
                    else:
                        vec = lose_vec[len(lose_vec)-1]
                        vec[1] = vec[1] + 1    
        break
    cur.close()

#分析数据    
def main_do_analysis_data():
    localtime = time.localtime(time.time())
    table = TABLE + "_" + str(localtime.tm_year)
    global con
    cur = con.cursor()
    ret_map = {}
    num = 0
    while True:
        logger.info("[do_analysis_data] 分析数据开始")
        sql = "select f_id from {0}".format(table)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        for id_vec in ret_data:
            do_analysis_one_stock(table,id_vec[0],ret_map)
            num = num + 1
        break
    cur.close()
    for key in ret_map:
        condition_vec = ret_map[key]
        logger.info("[do_analysis_data] 股票 {0} 分析开始".format(key))
        win_vec = condition_vec[0]
        for unit in win_vec:
            logger.info("[do_analysis_data] 股票 {0} 从 {1} 开始连续涨 {2} 次".format(key,unit[0],unit[1]))
            if unit[1] >= WIN_NUM:
                logger.warning("[do_analysis_data] 注意连涨 股票 {0} 从 {1} 开始连续涨 {2} 次".format(key,unit[0],unit[1]))
        lose_vec = condition_vec[1]
        for unit in lose_vec:
            logger.info("[do_analysis_data] 股票 {0} 从 {1} 开始连续跌 {2} 次".format(key,unit[0],unit[1]))
            if unit[1] >= LOSE_NUM:
                logger.warning("[do_analysis_data] 注意连跌 股票 {0} 从 {1} 开始连续跌 {2} 次".format(key,unit[0],unit[1]))
        logger.info("[do_analysis_data] 股票 {0} 分析结束".format(key))
    logger.info("[do_analysis_data] 分析 {0} 数据结束".format(num))

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
            logger.info("[tick_id_date_repeat_data] 剔除重复数据成功 {0},{1}".format(id,date))
        break
    cur.close()
    if not repeat_flag:
        logger.info("[tick_id_date_repeat_data] 没有剔除的重复数据 {0},{1}".format(id,date))

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
        logger.info("[tick_repeat_data] 剔除重复数据开始")
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
    logger.info("[tick_repeat_data] 剔除 {0} 重复数据结束".format(num))

##########################################################################剔除重复数据结束#############################################################



def init_log():
    global logger
    localtime = time.localtime(time.time())
    date = str(localtime.tm_mon) + "_" + str(localtime.tm_mday)
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)

    log_file = "./history_log/" + date + ".log"
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s-%(levelname)s-%(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)


     
if __name__ == "__main__":
    begin_time = time.time()
    while True:
        if len(sys.argv) < 2:
            logger.info("[main] 参数数量不正确 {0} 提示 collect,analysis,tick".format(sys.argv))
            break
        connect_mysql()    
        init_log()
        arg = sys.argv[1]
        if arg == "collect":
            main_do_date_today()
        elif arg == "analysis":
            main_do_analysis_data()
        elif arg == "tick":
            main_tick_repeat_data()
        else:
            logger.info("[main] 参数错误 {0} 提示 collect,analysis,tick".format(sys.argv))
        close_mysql()
        break
    logger.info("[main] 处理花费时间 {0} 秒".format(time.time()-begin_time))
