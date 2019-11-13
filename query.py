# -*- coding: utf-8 -*-

import easyquotation
import time
import json

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


def connect_mysql():
    global con
    con = pymysql.connect(host=HOST,port=PORT,user=USER,passwd=PASSWD,db=DB,charset=CHARSET)

def close_mysql():
    global con
    if con:
        con.close()
        print("[关闭数据库连接]")


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
        print("[create_stock_table] {0} 表已经存在".format(table))
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
    print("[create_stock_table] {0} 表创建成功".format(table))

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
        print("[add_date_column] {0} 列已经存在".format(date))
        return
    global con
    cur = con.cursor()
    sql = "alter table {0} add column {1} varchar(100000) NOT NULL COMMENT '日期'".format(table,date)
    cur.execute(sql)
    cur.close()
    print("[add_date_column] {0} 列创建成功".format(date))


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
    
    
def insert_or_update_id_data(table,date,id,name,dict_data):
    global con
    data = str(dict_data)
    if not check_id_data(table,id):
        cur = con.cursor()
        sql = 'insert into {0} (f_id, f_name,{1})values("{2}","{3}","{4}")'.format(table,date,id,name,data)
        if cur.execute(sql) == 1:
            print("[insert_or_update_id_data] 插入成功 {0},{1}".format(name,dict_data["date"]))
        cur.close()
    else:
        old_data = get_id_date_data(table,date,id)
        cur = con.cursor()
        cur_data = old_data + "?" + data
        sql = 'update {0} set {1} = "{2}" where f_id = "{3}"'.format(table,date,cur_data,id)
        if cur.execute(sql) == 1:
            print("[insert_or_update_id_data] 更新成功 {0},{1}".format(name,dict_data["date"]))
        cur.close()
  
    quotation = easyquotation.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    data_vec = quotation.market_snapshot(prefix=True)
    connect_mysql()
    check_time,table_flag,date_flag,table,date,date_vec = False,False,False,None,None,None
    localtime = time.localtime(time.time())
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
                        print("[do_date_today] 今天还没有数据 {0} {1} ".format(day,localtime.tm_mday))
                        break
            check_time = True
        if not table:
            table = TABLE + "_" + date_vec[0]
        if not date_flag:
            date = DATE + date_vec[1] + "_" + date_vec[2]
        if not table_flag:
            create_stock_table(table)
            table_flag = True
        if not date_flag:
            add_date_column(table,date)
            date_flag = True
        if stock_vec["name"] == "世纪华通":
            insert_or_update_id_data(table,date,id,stock_vec["name"],stock_vec)
            
    print("[do_date_today] {0} 数据已处理完毕".format(date))
   
#处理当天所有的股票数据
def do_date_today():
    quotation = easyquotation.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    data_vec = quotation.market_snapshot(prefix=True)
    connect_mysql()
    check_time,table_flag,date_flag,table,date,date_vec = False,False,False,None,None,None
    localtime = time.localtime(time.time())
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
                        print("[do_date_today] 今天还没有数据 {0} {1} ".format(day,localtime.tm_mday))
                        break
            check_time = True
        if not table:
            table = TABLE + "_" + date_vec[0]
        if not date_flag:
            date = DATE + date_vec[1] + "_" + date_vec[2]
        if not table_flag:
            create_stock_table(table)
            table_flag = True
        if not date_flag:
            add_date_column(table,date)
            date_flag = True
        if stock_vec["name"] == "世纪华通":
            insert_or_update_id_data(table,date,id,stock_vec["name"],stock_vec)
            
    print("[do_date_today] {0} 数据已处理完毕".format(date))


#判断一天的走势，返回，当天是否涨(True),下午的开价
def compare_same_data(data_str):
    ret_vec = []
    data_str_vec = data_str.split("?")
    for data in data_str_vec:
        if data:
            ret_vec.append(eval(data))
    begin_end_vec = [False,ret_vec[len(ret_vec)-1]]
    #现价大于今日开盘价和大于昨日收盘价
    if begin_end_vec[1]["now"] > begin_end_vec[1]["open"] and begin_end_vec[1]["now"] > begin_end_vec[1]["close"]:
        begin_end_vec[0] = True
    return begin_end_vec
    
def do_analysis_one_stock(table,id):
    ret_map = {}
    while True:
        global con
        cur = con.cursor()
        sql = "select * from {0} where f_id = '{1}'".format(table,id)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        for data_vec in ret_data:
            for i in range(2,len(data_vec)):
                day_begin_end_vec = compare_same_data(data_vec[i])
                ret_map[day_begin_end_vec[1]] = day_begin_end_vec[0]
        break
    cur.close()
    for key in ret_map:
        
    
def do_analysis_data():
    connect_mysql()
    localtime = time.localtime(time.time())
    table = TABLE + "_" + str(localtime.tm_year)
    while True:
        global con
        cur = con.cursor()
        sql = "select f_id from {0}".format(table)
        if cur.execute(sql) <= 0:
            break
        ret_data = cur.fetchall()
        for id_vec in ret_data:
            print("id:",id_vec[0])
            do_analysis_one_stock(table,id_vec[0])
        break
    cur.close()

if __name__ == "__main__":
    do_analysis_data()
    #do_date_today()
