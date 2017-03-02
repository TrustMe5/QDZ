
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

def insertToDB(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute(sql)
    conn.commit()
    cur.close()

if __name__ == '__main__':
    time1, time2 = getTime()
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    sql5 = "select count(*) from djuniqueperson where update_date >= '%s'" % (today)
    sql6 = "select count(*) from fjl.resume where update_time >= '%s'" % (today)
    num_uniper = getDBdataFrom69(sql5, "192.168.6.69", "test", "test", "resume_update")[0][0]
    num_120fjl = getDBdataFromPostgree(sql6, "192.168.6.120", "postgres", "5432@pk_id", "fjl_resume")[0][0]
    diff_num = num_uniper - num_120fjl
    sql7 = "insert into uniper_120fjl_num (time, value) values ('{0}', '{1}')".format(time2, diff_num)
    insertToDB(sql7, '10.18.99.179', 'root', 'root', 'monitor_info')
    print num_uniper, num_120fjl