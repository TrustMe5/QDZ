#coding=utf-8
import datetime,time
from pymongo import MongoClient
import MySQLdb,logging,os
import psycopg2

def initLogging(logname):
    fmt = '%(asctime)s %(name)s %(filename)s(%(funcName)s[line:%(lineno)d]) %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG,
                        format=fmt,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join('logs', '{0}.log'.format(logname)),
                        filemode='a'
                        )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def getTodaytimestamp():
    now_time = datetime.datetime.now().strftime('%Y%m%d')
    today_timestamp = time.mktime(time.strptime(now_time, '%Y%m%d'))
    return today_timestamp

def getMongoDataNum(dbname, collection_name):
    db1 = dbname
    collection1 = collection_name
    print db1, collection1
    client = MongoClient('mongodb://192.168.6.97:40000/')
    db = client["'"+db1+"'"]
    collection1 = db["'"+collection1+"'"]
    #newzengliangdata = collection1.find({"create_time": {"$gt":1472227200, "$lt":1472017128}})
    newzengliangdata = collection1.find({"create_time": {"$ge": getTodaytimestamp()}})
    print newzengliangdata.count()
    client.close()

def getDBdata(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    return data

def getDBdataFromPostgree(sql, host, user, passwd, db):
    conn = psycopg2.connect(database=db, user=user, password=passwd, host=host, port="5432")
    cur = conn.cursor()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    conn.close()
    return data

def getCurrentTime():
    now_time = datetime.datetime.now()
    return now_time.strftime("%Y-%m-%d %H:%M:%S")

def getTodayDate():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def getBefo30dayDate():
    now_time = datetime.datetime.now()
    befo_time = now_time + datetime.timedelta(days=-30)
    return befo_time.strftime("%Y.%m.%d")

def insertToDB(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute(sql)
    conn.commit()
    cur.close()



if __name__=='__main__':

    initLogging("monitor_send_" + getTodayDate().replace("-", ""))
    logging.info("start...")
    today_date = getTodayDate()
    befo30day_date = getBefo30dayDate()
    todaydatestamp = getTodaytimestamp()
    logging.info("todaydatestamp: {0}".format(todaydatestamp))
    # getMongoDataNum("zhengliangdb", "newzengliangdata")

    client = MongoClient('mongodb://192.168.6.97:40000/')
    db = client.zhengliangdb
    collection = db.newzengliang
    # newzengliangdata = collection1.find({"create_time": {"$gt":1472227200, "$lt":1472017128}})
    newzengliangdata = collection.find({"create_time": {"$gte": todaydatestamp}})
    num1 = newzengliangdata.count()
    logging.info("zhengliangdb中的newzengliang数目：{0}".format(num1))

    db = client.dajieresumedb
    collection = db.youlian
    num2 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("dajieresumedb中的youlian数目：{0}".format(num2))

    db = client.dajieresumedb
    collection = db.wulian
    num3 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("dajieresumedb中的wulian数目：{0}".format(num3))

    db = client.dajieresumedb
    collection = db.middle
    num4 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("dajieresumedb中的middle数目：{0}".format(num4))

    db = client.zhengliangdb
    collection = db.zengliangwulian
    num5 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("zhengliangdb中的zengliangwulian数目：{0}".format(num5))

    db = client.static_data
    collection = db.liepin160526_youlian
    num6 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("static_data中的liepin160526_youlian数目：{0}".format(num6))

    db = client.static_data
    collection = db.liepin160526_wulian
    num7 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("static_data中的liepin160526_wulian数目：{0}".format(num7))

    db = client.static_data
    collection = db.youlian
    num8 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("static_data中的youlian数目：{0}".format(num8))

    db = client.static_data
    collection = db.wulian
    num9 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("static_data中的的wulian数目：{0}".format(num9))

    db = client.splice_data
    collection = db.youlian
    num10 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("splice_data中的youlian数目：{0}".format(num10))

    db = client.emaildb
    collection = db.temp
    num11 = collection.find({"create_time": {"$gte": todaydatestamp}}).count()
    logging.info("emaildb中的temp数目：{0}".format(num11))

    client.close()

    total = num1 + num2 + num3 + num4 + num5 + num6 + num7 + num8 + num9 + num10 + num11
    logging.info("今日mongo中总数为：{0}".format(total))

    tableName = "original_resume_data_v2_" + str(today_date.replace("-", ""))
    logging.info("查询6.63上mergedata01数据库中{0}的时间为：{1}".format(tableName, getCurrentTime()))
    sql = "select count(*) from %s" % (tableName)
    logging.info("sql: {0}".format(sql))
    datanum = getDBdata(sql, '192.168.6.63', 'root', 'qiaodadata', 'mergedata01')         # 6.63上 mergedata01 数据库中的数目
    logging.info("{0}表中的数量为：{1}".format(tableName, datanum[0][0]))

    cur_time = getCurrentTime()
    diff_num = total - datanum[0][0]     # mongodb与数据库中的数量之差
    logging.info("mongo今日数据量与db的数量之差为：{0}".format(diff_num))

    logging.info("将mongo今日数据量与db的数量之差存入数据库，时间：{0}".format(getCurrentTime()))
    sql2 = "insert into mongodb_num(time, value) values ('%s', '%s')" % (cur_time, diff_num)
    logging.info("sql2: {0}".format(sql2))
    # insertToDB(sql2, '10.18.99.179', 'root', 'root', 'monitor_info')   # 存入数据库

    logging.info("开始查询今日自然人表数量，时间：{0}".format(getCurrentTime()))
    sql3 = "select count(*) from djuniqueperson where update_date >= '%s'" % (today_date)
    logging.info("sql3: {0}".format(sql3))
    uniper_num = getDBdata(sql3, '192.168.6.69', 'test', 'test', 'resume_update')
    logging.info("今日更新自然人表数量为：{0}".format(uniper_num[0][0]))

    logging.info("开始查询今日djpendingdata表数量，时间：{0}".format(getCurrentTime()))
    sql4 = "select count(*) from djpendingdata where insert_date >= '%s'" % (today_date)
    logging.info("sql4: {0}".format(sql4))
    djpending_num = getDBdata(sql4, '192.168.6.69', 'test', 'test', 'resume_update')
    logging.info("今日djpendingdata表数量，：{0}".format(djpending_num[0][0]))
    uniper_djpend_num = uniper_num[0][0] - djpending_num[0][0]
    logging.info("今日自然人表与djpendingdata的差值为：{0}".format(uniper_djpend_num))

    logging.info("开始查询今日djuniqueperson表近30天的数据数量，时间：{0}".format(getCurrentTime()))
    sql8 = "select count(*) from djuniqueperson where update_date >= '%s' and version_date >= '%s' and version_date <= '%s'" % (today_date, befo30day_date, today_date.replace("-", "."))
    logging.info("sql8: {0}".format(sql8))
    uniper_num2 = getDBdata(sql8, '192.168.6.69', 'test', 'test', 'resume_update')
    logging.info("今日djuniqueperson表近30天的数据数量为：{0}".format(uniper_num2[0][0]))

    logging.info("开始查询今日ycpendingdata表的数据数量，时间：{0}".format(getCurrentTime()))
    sql5 = "select count(*) from ycpendingdata where insert_date >= '%s'" % (today_date)
    logging.info("sql5: {0}".format(sql5))
    ycpending_num = getDBdata(sql5, '192.168.6.69', 'test', 'test', 'resume_update')
    logging.info("今日ycpendingdata表的数据数量为：{0}".format(ycpending_num[0][0]))
    uniper_ycpend_num = uniper_num2[0][0] - ycpending_num[0][0]
    logging.info("今日djuniqueperson与ycpendingdata的数据数量之差：{0}".format(uniper_ycpend_num))

    logging.info("开始将djuniqueperson与djpendingdata的差值存入数据库，时间为：{0}".format(getCurrentTime()))
    sql6 = "insert into uniper_djpend_num(time, value) values ('%s', '%s')" % (cur_time, uniper_djpend_num)
    logging.info("sql6: {0}".format(sql6))
    # insertToDB(sql6, '10.18.99.179', 'root', 'root', 'monitor_info')   # 存入数据库

    logging.info("开始将djuniqueperson与ycpendingdata的差值存入数据库，时间为：{0}".format(getCurrentTime()))
    sql7 = "insert into uniper_ycpend_num(time, value) values ('%s', '%s')" % (cur_time, uniper_ycpend_num)
    logging.info("sql7: {0}".format(sql7))
    # insertToDB(sql7, '10.18.99.179', 'root', 'root', 'monitor_info')   # 存入数据库


    sql9 = "select count(*) from fjl.resume where update_time like '{0}%'" .format(today_date)
    logging.info("sql9: {0}".format(sql9))
    logging.info("开始查询120纷简历今日更新简历数量,时间为{0}".format(getCurrentTime()))
    num_120fjl = getDBdataFromPostgree(sql9, "192.168.6.120", "postgres", "5432@pk_id", "fjl_resume")[0][0]
    logging.info("120纷简历今日更新简历数量为：{0}".format(num_120fjl))

    diff_num = uniper_num[0][0] - num_120fjl
    logging.info("自然人表今日更新数据量与120纷简历今日更新简历数量的差值为：{0}".format(diff_num))

    sql11 = "insert into uniper_120fjl_num (time, value) values ('{0}', '{1}')".format(cur_time, diff_num)
    logging.info("sql11:{0}".format(sql11))
    logging.info("将差值存入数据库.")
    # insertToDB(sql11, '10.18.99.179', 'root', 'root', 'monitor_info')

    logging.info("开始查询今日大街和英才的未发送数目.")
    sql12 = "select count(*) from djpendingdata where insert_date like '{0}%' and sent = '0'".format(today_date)   #今日大街待发送数目
    sql13 = "select count(*) from ycpendingdata where insert_date like '{0}%' and sent = '0'".format(today_date)   #今日英才待发送数目
    logging.info("sql12: {0}".format(sql12))
    logging.info("sql13: {0}".format(sql13))
    dj_notsent_num = getDBdata(sql12, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
    yc_notsent_num = getDBdata(sql13, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
    logging.info("今日大街的未发送数目为：{0}, 今日英才未发送的数目为：{1}".format(dj_notsent_num, yc_notsent_num))
    logging.info("把未发送数目插入数据库.")
    sql14 = "insert into dj_notsent_num(time, value) values ('%s', '%s')" % (cur_time, dj_notsent_num)
    sql15 = "insert into yc_notsent_num(time, value) values ('%s', '%s')" % (cur_time, yc_notsent_num)
    logging.info("sql14: {0}".format(sql14))
    logging.info("sql15: {0}".format(sql15))

    # insertToDB(sql14, '10.18.99.179', 'root', 'root', 'monitor_info')
    # insertToDB(sql15, '10.18.99.179', 'root', 'root', 'monitor_info')


    sql16 = "select count(*) from {0} where LENGTH(unified_date) = 20 and LENGTH(push_date) = 0".format(tableName)
    waitsyn_num = getDBdata(sql16, '192.168.6.63', 'root', 'qiaodadata', 'mergedata01')[0][0]
    print waitsyn_num
    sql17 = "insert into wait_synchr_num(time, value) values ('%s', '%s')" % (cur_time, waitsyn_num)
    insertToDB(sql17, '10.18.99.179', 'root', 'root', 'monitor_info')

    sql18 = "select count(*) from djpendingdata where sent = '0'"     #查询大街待发送的总数目
    sql19 = "select count(*) from ycpendingdata where sent = '0'"      #查询英才待发送的总数目
    logging.info("sql18:{0}".format(sql18))
    logging.info("sql19:{0}".format(sql19))
    dj_notsent_totalnum = getDBdata(sql18, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
    yc_notsent_totalnum = getDBdata(sql19, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
    sql20 = "insert into dj_notsent_num_total(time, value) values ('%s', '%s')" % (cur_time, dj_notsent_totalnum)
    sql21 = "insert into yc_notsent_num_total(time, value) values ('%s', '%s')" % (cur_time, yc_notsent_totalnum)
    logging.info("sql20: {0}".format(sql20))
    logging.info("sql21: {0}".format(sql21))
    insertToDB(sql20, '10.18.99.179', 'root', 'root', 'monitor_info')
    insertToDB(sql21, '10.18.99.179', 'root', 'root', 'monitor_info')
    logging.info("存储完成.")


    logging.info("end...")







