#coding=utf-8
import MySQLdb
import datetime

def getCurDate():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d')

def getDBdata(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    return data

def insertToDB(cur_date, sql_insert, sql_update, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    sql = "select time from fjl_enterdb_info where time = '{0}'".format(cur_date)
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
    print datetime.datetime.now()
    cur_date = getCurDate()
    print cur_date
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = "select sent, count(sent) from fjlpendingdata where insert_date like '{0}%' GROUP BY sent".format(cur_date)
    datalist = getDBdata(sql, '192.168.6.65', 'root', 'root', 'resume_update')
    msg = ''
    for data in datalist:
        msg += '"' + str(data[0]) + '"' + ":" + '"' + str(data[1]) + '",'
    msg = "{" + msg[:len(msg) - 1] + "}"
    print msg
    sql_insert = "insert into fjl_enterdb_info(time, num_info) values('{0}','{1}')".format(now_time, msg.encode('utf-8'))
    sql_update = "update fjl_enterdb_info set num_info = '{0}' where time = '{1}'".format(msg.encode('utf-8'), now_time)
    insertToDB(now_time, sql_insert, sql_update, '10.18.99.179', 'root', 'root', 'monitor_info')
    print 'end.\n'



