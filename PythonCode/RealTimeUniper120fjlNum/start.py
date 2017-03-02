#coding=utf-8
import MySQLdb
import psycopg2
import sys,datetime
reload(sys)
sys.setdefaultencoding('utf8')

def getTime():
    now_time = datetime.datetime.now()
    onehourbefore_time = now_time + datetime.timedelta(hours=-1)
    return onehourbefore_time.strftime('%Y-%m-%d %H:%M:%S'),now_time.strftime('%Y-%m-%d %H:%M:%S')


def getDBdataFrom69(sql, host, user, passwd, db):
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

def insertToDB(time, sql_insert, sql_update, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    sql2 = "select time from realtime_uniper_120fjl_num where time like '{0}%'".format(time[:13])
    oper = cur.execute(sql2)
    data = cur.fetchmany(oper)
    if data:
        print 'update.'
        cur = conn.cursor()
        cur.execute(sql_update)
        conn.commit()
        cur.close()
    else:
        print 'insert.'
        cur = conn.cursor()
        cur.execute(sql_insert)
        conn.commit()
        cur.close()


if __name__ == '__main__':
    # time1, time2 = getTime()
    time2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if ":30:" in time2:
        time1 = datetime.datetime.now() + datetime.timedelta(seconds=-1800)
    else:
        time1 = datetime.datetime.now() + datetime.timedelta(hours=-1)
    # time1 = datetime.datetime.now() + datetime.timedelta
    time1 = time1.strftime("%Y-%m-%d %H:%M:%S")
    print "time1:{0}, time2:{1}".format(time1, time2)
    print time1[:13]
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    sql1 = "select count(*) from djuniqueperson where update_date >= '%s' and update_date <= '%s'" % (time1, time2)
    sql2 = "select count(*) from fjl.resume where update_time >= '%s' and update_time <= '%s' and is_delete = 'f'" % (time1, time2)
    datanum1 = getDBdataFrom69(sql1, "192.168.6.69", "test", "test", "resume_update")[0][0]           #最近一小时自然人表数量
    datanum2 = getDBdataFromPostgree(sql2, "192.168.6.120", "postgres", "5432@pk_id", "fjl_resume")[0][0]         #最近一小时 120fjl 表数量
    print "最近一小时自然人表数量为：{0}, 最近一小时120fjl表数量为：{1}".format(datanum1, datanum2)

    # sendmail.send_mail(time1, time2, datanum1[0][0], datanum2[0][0])
    # print '自然人表中从%s到%s的数量：%s'%(time1, time2, datanum1[0][0])
    # print '纷简历表中从%s到%s的数量：%s'%(time1, time2, datanum2[0][0])

    sql5 = "select count(*) from djuniqueperson where update_date >= '%s'" % (today)
    sql6 = "select count(*) from fjl.resume where update_time >= '%s'" % (today)
    num_uniper = getDBdataFrom69(sql5, "192.168.6.69", "test", "test", "resume_update")[0][0]     #今日自然人表总的数量
    num_120fjl = getDBdataFromPostgree(sql6, "192.168.6.120", "postgres", "5432@pk_id", "fjl_resume")[0][0]   #今日120fjl表总的数量
    diff_num = num_uniper - num_120fjl
    sql7 = "insert into uniper_120fjl_num (time, value) values ('{0}', '{1}')".format(time2, diff_num)
    # insertToDB(sql7, '10.18.99.179', 'root', 'root', 'monitor_info')
    print "今日自然人表总量为：{0}, 今日120fjl表总量为：{1}".format(num_uniper, num_120fjl)

    sql_insert = "insert into realtime_uniper_120fjl_num(time, recenthour_unipernum, recenthour_120fjlnum, uniper_cumulatenum, 120fjl_cumulatenum, update_time) values('%s', '%s', '%s', '%s', '%s', '%s')" % (time1, datanum1, datanum2, num_uniper, num_120fjl, time2)
    sql_update = "update realtime_uniper_120fjl_num set recenthour_unipernum = '{0}', recenthour_120fjlnum = '{1}', uniper_cumulatenum = '{2}', 120fjl_cumulatenum = '{3}', update_time = '{4}' where time like '{5}%'".format(datanum1, datanum2, num_uniper, num_120fjl, time2, time1[:13])
    print sql_insert
    print sql_update
    # insertToDB(sql3, '10.18.99.179', 'root', 'root', 'monitor_info')
    insertToDB(time1, sql_insert, sql_update, '10.18.99.179', 'root', 'root', 'monitor_info')

