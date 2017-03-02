#coding=utf-8
import MySQLdb
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def getDBdata(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    return data

def getCurrentTime():
    now_time = datetime.datetime.now()
    # befo_time = now_time + datetime.timedelta(seconds=-9000)
    return now_time.strftime("%Y-%m-%d %H:%M:%S")

def insertToDB(today_date, sql_update, sql_insert, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    sql = "select date from rocketpushstatistic where date = '{0}'".format(today_date)
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    if data:
        print 'update'
        cur = conn.cursor()
        cur.execute(sql_update)
        conn.commit()
        cur.close()
    else:
        print 'insert'
        cur = conn.cursor()
        cur.execute(sql_insert)
        conn.commit()
        cur.close()



if __name__ == '__main__':
    cur_time = getCurrentTime()
    today_date = cur_time[:10]

    sql1 = "select count(*) from rocketpendingdata WHERE sent != '0' and send_date like '{0}%'".format(today_date)   #当天一共发送的数目
    sent_totalnum = getDBdata(sql1,  "192.168.6.65", 'root', 'root', 'resume_update')[0][0]

    sql2 = "select count(*) from rocketpendingdata WHERE sent = '1' and send_date like '{0}%'".format(today_date)   #当天发送成功的数目
    sent_succnum = getDBdata(sql2,  "192.168.6.65", 'root', 'root', 'resume_update')[0][0]

    sent_failnum = sent_totalnum - sent_succnum          #发送失败数

    print sent_totalnum, sent_succnum, sent_failnum
    sql_update = "update rocketpushstatistic set sent_totalnum = '{0}', sent_succnum = '{1}', sent_failnum = '{2}', real_time = '{3}' where date = '{4}'".format(sent_totalnum, sent_succnum, sent_failnum, cur_time, today_date)
    sql_insert = "insert into rocketpushstatistic (date, sent_totalnum, sent_succnum, sent_failnum, real_time) values ('{0}','{1}','{2}','{3}','{4}')".format(today_date, sent_totalnum, sent_succnum, sent_failnum, cur_time)
    print sql_insert
    insertToDB(today_date, sql_update, sql_insert, "192.168.6.65", 'root', 'root', 'resume_update')



