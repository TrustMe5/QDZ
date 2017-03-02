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
    print "cur_time:{0}".format(cur_time)

  #  sql1 = "select count(*) from rocketpendingdata WHERE send_date like '{0}%' and sent != '0'".format(today_date)   #当天一共处理的数目
  #  process_totalnum = getDBdata(sql1,  "192.168.6.65", 'root', 'root', 'resume_update')[0][0]
  #  print "处理总数:{0}".format(process_totalnum)

    sql2 = "select count(*) from rocketpendingdata WHERE send_date like '{0}%' and sent = '1' ".format(today_date)   #当天发送成功的数目
    sent_succnum = getDBdata(sql2,  "192.168.6.65", 'root', 'root', 'resume_update')[0][0]
    print "成功发送数:{0}".format(sent_succnum)

    
    sql3 = "select count(*) from rocketpendingdata where send_date like '{0}%' and (sent = '5-10001' or sent = '5-10002'  or sent = '5-10003'  or sent = '5-10004' or sent = '5-10006')".format(today_date)
    print sql3
    sent_failnum = getDBdata(sql3,  "192.168.6.65", 'root', 'root', 'resume_update')[0][0]          #发送失败数
    print "发送失败数:{0}".format(sent_failnum)


    sql_update = "update rocketpushstatistic set sent_succnum = '{0}', sent_failnum = '{1}', real_time = '{2}' where date = '{3}'".format(sent_succnum, sent_failnum, cur_time, today_date)
    sql_insert = "insert into rocketpushstatistic (date, sent_succnum, sent_failnum, real_time) values ('{0}','{1}','{2}','{3}')".format(today_date, sent_succnum, sent_failnum, cur_time)
    print sql_update
    print sql_insert
    insertToDB(today_date, sql_update, sql_insert, "192.168.6.65", 'root', 'root', 'resume_update')
    print "存入数据库成功."
    print "\n"




