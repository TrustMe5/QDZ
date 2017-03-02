#coding=utf-8
import MySQLdb
import datetime, time
import psycopg2,urllib2
import httplib, urllib, json, requests

def getDate():
    now = datetime.datetime.now()
    threedaysbef = now + datetime.timedelta(days=-2)
    return threedaysbef.strftime('%Y.%m.%d'), now.strftime('%Y.%m.%d')

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
    sql = "select time from enterdb_info where time = '{0}'".format(cur_date)
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

def getDBdataFromPostgree(sql, host, user, passwd, db):
    conn = psycopg2.connect(database=db, user=user, password=passwd, host=host, port="5432")
    cur = conn.cursor()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    conn.close()
    return data


def savetofile(content, day):
    filename = "./interf_content/" + datetime.datetime.now().strftime('%Y-%m-%d-%H') + "_rec_{0}.log".format(day)
    print filename
    with open(filename, 'wb') as wp:
        wp.write(content)
        wp.close()


#def postweb(url, updateDate):
def postweb(url, updateDate, startRtime, endRtime):
    httpClient = None
    try:
 #       params = urllib.urlencode({"from": "c0695f801d695365247b045daa07c034", "updateDate": updateDate, "offset": "0", "rows": "30", "sortBy": "1", "sortType": "1"})
        params = urllib.urlencode({"from": "c0695f801d695365247b045daa07c034", "startRefreshTime": startRtime, "endRefreshTime": endRtime, "offset": "0", "rows": "30", "sortBy": "1", "sortType": "1"})
        # headers = {"Content-type": "application/x-www-form-urlencoded"
        #     , "Accept": "text/plain"}
        request = urllib2.Request(url, params)
        response = urllib2.urlopen(request)
        content = response.read()
        savetofile(content, updateDate)
        return json.loads(content)['totalSize']
    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close()

if __name__ == '__main__':
    threedaysbef, today_date = getDate()
    print threedaysbef, today_date

  #  threedaysbef_stamp = time.mktime(time.strptime(threedaysbef, '%Y.%m.%d'))
  #  curtime_stamp = time.mktime(datetime.datetime.now().timetuple())

    threedaysbef_stamp = str(int(time.mktime(time.strptime(threedaysbef, '%Y.%m.%d'))))
    curtime_stamp = str(int(time.mktime(datetime.datetime.now().timetuple())))
    print threedaysbef_stamp, curtime_stamp
    today_stamp = str(int(time.mktime(datetime.date.today().timetuple())))      #获取当天0点0分0秒的时间戳
    
    cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')            #查找近三天自然人表数量
    print '当前时间：', cur_time
    sql = "select count(*) from djuniqueperson where version_date >= '{0}' and version_date <= '{1}'".format(threedaysbef, today_date)
    uniper_recent3day_num = getDBdata(sql, '192.168.6.69', 'root', 'root', 'resume_update')[0][0]
    print "uniper_recent3day_num: {0}".format(uniper_recent3day_num)

    sql2 = "select count(*) from djuniqueperson where version_date = '{0}'".format(today_date)
    uniper_recent1day_num = getDBdata(sql2, '192.168.6.69', 'root', 'root', 'resume_update')[0][0]
    print "uniper_recent1day_num: ", uniper_recent1day_num

    fjl_interface_url = "http://192.168.6.61/resume/api/search"
  #  fjl_recent3day_num = postweb(fjl_interface_url, "3")        #纷简历接口返回的近三天数
    fjl_recent3day_num = postweb(fjl_interface_url, "3", threedaysbef_stamp, curtime_stamp)        #纷简历接口返回的近三天数
    print "fjl_recent3day_num: {0}".format(fjl_recent3day_num)

#    fjl_recent1day_num = postweb(fjl_interface_url, "1")
    fjl_recent1day_num = postweb(fjl_interface_url, "1", today_stamp, curtime_stamp)
    print "fjl_recent1day_num: {0}".format(fjl_recent1day_num)


    # sql3 = "select count(*) from fjl.resume where refresh_time >= {0} and refresh_time <= {1}".format(threedaysbef_stamp, curtime_stamp)
    # num_120fjl = getDBdataFromPostgree(sql2, "192.168.6.120", "postgres", "5432@pk_id", "fjl_resume")[0][0]
    # print num_120fjl


    sql_insert = "insert into enterdb_info(time, uniper_recent1day_num, uniper_recent3day_num, fjl_recent1day_num, fjl_recent3day_num) values('{0}', '{1}', '{2}', '{3}', '{4}')".format(cur_time, uniper_recent1day_num, uniper_recent3day_num, fjl_recent1day_num, fjl_recent3day_num)
    insertToDB(cur_time, sql_insert, "", '10.18.99.179', 'root', 'root', 'monitor_info')
    print 'end,\n'




