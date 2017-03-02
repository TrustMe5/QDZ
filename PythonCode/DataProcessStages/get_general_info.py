#coding=utf-8
import MySQLdb
import datetime
import sys, json
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
    befo_time = now_time + datetime.timedelta(seconds=-900)
    return befo_time.strftime("%Y-%m-%d %H:%M:%S"), now_time.strftime("%Y-%m-%d %H:%M:%S")

def insertToDB(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute(sql)
    conn.commit()
    cur.close()


if __name__ == '__main__':
    befo_time, cur_time = getCurrentTime()
    sql = "select tname, description from master_ctrl"
    tablelist = getDBdata(sql, '10.18.99.179', 'root', 'root', 'monitor_info')
    dic_num = dict()
    msg = ''
    for tname in tablelist:
        sql1 = "select value from %s where time <= '%s' and time >= '%s' order by time desc limit 1"%(tname[0], cur_time, befo_time)
        valuelist = getDBdata(sql1, '10.18.99.179', 'root', 'root', 'monitor_info')
        for value in valuelist:
            # msg += "'{}':'{}',".format(tname[1], value[0])
            msg += '"' + tname[1]+'"' +":" + '"'+value[0] + '",'
            dic_num[tname[1]] = value[0]
    msg = "{" + msg[:len(msg)-1] + "}"
    sql2 = "insert into general_info(time, num_info) values ('%s', '%s')" % (cur_time, msg.encode("utf-8"))
    print sql2
    insertToDB(sql2, '10.18.99.179', 'root', 'root', 'monitor_info')
    print 'end.'


