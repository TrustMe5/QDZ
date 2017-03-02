#coding= utf-8
import MySQLdb,datetime,csv
import sys, re, datetime, time
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

def getTime():
    now_time = datetime.datetime.now()
    return now_time.strftime('%Y-%m-%d')
    # return '2016-10-' + day

def writeToCSV(datalist, filename):
    with open(filename, 'ab') as wp:
        writer = csv.writer(wp)
        # writer.writerow(['totalnum', 'norepeatnum'])
        writer.writerow(datalist)

def insertIntoDB(time, zlnum, qcnum, lpnum, totalnum, parsenum, fdfsnum, successnum, failnum, mongonum, zl_succnum, qc_succnum, lp_succnum, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num):
    conn = MySQLdb.connect(host="192.168.6.80", user="root", passwd="root", db="email_process", charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    sql = "select time from col_parse_info where time = '{0}'".format(time)
    # cur.execute("select time from col_parse_info where time = '{0}'".format(time))
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    if data:
        print 'update'
        cur = conn.cursor()
        sql = "update col_parse_info set col_zl_num = '{0}', col_qc_num = '{1}', col_lp_num = '{2}', col_total_num = '{3}', parse_num = '{4}', enter_fdfs_num = '{5}', parse_succ_num = '{6}', parse_fail_num = '{7}', enter_mongo_num = '{8}', zl_parsesucc_num = '{9}', qc_parsesucc_num = '{10}', lp_parsesucc_num = '{11}', zl_savecol_num = '{12}', zl_addcol_num = '{13}', qc_savecol_num = '{14}', qc_addcol_num = '{15}', lp_savecol_num = '{16}', lp_addcol_num = '{17}' where time = '{18}'".format(zlnum, qcnum, lpnum, totalnum, parsenum, fdfsnum, successnum, failnum, mongonum, zl_succnum, qc_succnum, lp_succnum, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num, time)
        cur.execute(sql)
        conn.commit()
        cur.close()
    else:
        print 'insert'
        cur = conn.cursor()
        cur.execute("insert into  col_parse_info(time, col_zl_num, col_qc_num, col_lp_num, col_total_num, parse_num, enter_fdfs_num, parse_succ_num, parse_fail_num, enter_mongo_num, zl_parsesucc_num, qc_parsesucc_num, lp_parsesucc_num, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(time, zlnum, qcnum, lpnum, totalnum, parsenum, fdfsnum, successnum, failnum, mongonum, zl_succnum, qc_succnum, lp_succnum, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num))
        conn.commit()
        cur.close()

def getSentTime(filename):
    pattern = re.compile('sentTime_(.*)_emailID_')
    time = pattern.findall(filename)[0]
    return int(time)

def checkIfBeyond30Days(date, senttime):
    now_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    old_time = now_time + datetime.timedelta(days=-30)
    old_time = old_time.strftime('%Y%m%d')
    old_timestamp = time.mktime(time.strptime(old_time, '%Y%m%d'))
    print senttime, old_timestamp
    if senttime >= int(old_timestamp):
        return 1
    else:
        return 0


if __name__ == '__main__':
        ##################采集情况
        print 'start'
        print "current_time: {0}".format(datetime.datetime.now())
        date = getTime()

        print date
        collect_tablename = "collector_" + date.replace("-", "")
        print collect_tablename
        sql_zl = "select count(*) from {0} where filename like '%_channel_1_%'".format(collect_tablename)
        sql_qc = "select count(*) from {0} where filename like '%_channel_2_%'".format(collect_tablename)
        sql_lp = "select count(*) from {0} where filename like '%_channel_3_%'".format(collect_tablename)
        sql_totalnum = "select count(*) from {0}".format(collect_tablename)
        sql_email = "select email, count(*) from {0} GROUP BY email".format(collect_tablename)

        sql_zlsavecolnum = "select count(*) from {0} where filename like '%_channel_1_%' and filename like '%_category_1_%'".format(collect_tablename)
        sql_zladdcolnum = "select count(*) from {0} where filename like '%_channel_1_%' and filename like '%_category_2_%'".format(collect_tablename)
        sql_qcsavecolnum = "select count(*) from {0} where filename like '%_channel_2_%' and filename like '%_category_1_%'".format(collect_tablename)
        sql_qcaddcolnum = "select count(*) from {0} where filename like '%_channel_2_%' and filename like '%_category_2_%'".format(collect_tablename)
        sql_lpsavecolnum = "select count(*) from {0} where filename like '%_channel_3_%' and filename like '%_category_1_%'".format(collect_tablename)
        sql_lpaddcolnum = "select count(*) from {0} where filename like '%_channel_3_%' and filename like '%_category_2_%'".format(collect_tablename)

        zl_savecol_num = getDBdata(sql_zlsavecolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        zl_addcol_num = getDBdata(sql_zladdcolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        qc_savecol_num = getDBdata(sql_qcsavecolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        qc_addcol_num = getDBdata(sql_qcaddcolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        lp_savecol_num = getDBdata(sql_lpsavecolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        lp_addcol_num = getDBdata(sql_lpaddcolnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]


        zlnum = getDBdata(sql_zl, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        qcnum = getDBdata(sql_qc, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        lpnum = getDBdata(sql_lp, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        totalnum = getDBdata(sql_totalnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        datalist = getDBdata(sql_email, '192.168.6.80', 'root', 'root', 'email_process')
        emailinfo = ""
        for data in datalist:
            emailinfo += "{0}:{1}, ".format(data[0], data[1])
        # writeToCSV([getTime(), zlnum, qcnum, lpnum, totalnum, emailinfo], "collect_info.csv")

        ##############解析情况
        parse_tablename = "email_" + date.replace("-", "")
        print parse_tablename
        sql_parsenum = "SELECT count(*) from {0}".format(parse_tablename)
        sql_fdfsnum = "select count(*) from {0} where fastdfs_url !=''".format(parse_tablename)
        sql_successnum = "select count(*) from {0} where parse_time !=''".format(parse_tablename)
        sql_failnum = "select count(*) from {0} where parse_time =''".format(parse_tablename)
        sql_mongonum = "select count(*) from {0} where objid !=''".format(parse_tablename)
        sql_zlsuccnum = "select count(*) from {0} where filename like '%_channel_1_%' and parse_time != ''  ".format(parse_tablename)
        sql_qcsuccnum = "select count(*) from {0} where filename like '%_channel_2_%' and parse_time != ''  ".format(parse_tablename)
        sql_lpsuccnum = "select count(*) from {0} where filename like '%_channel_3_%' and parse_time != ''  ".format(parse_tablename)

        sql_ycquenum = "select count(*) from ycpendingdata where insert_date like '{0}%' and source = '45'".format(date)
        sql_ycnewaddnum = "select count(*) from ycpendingdata where insert_date like '{0}%' and source = '45' and sent = '1' and usertype = '4'".format(date)
        sql_ycsendfailnum = "select count(*) from ycpendingdata where insert_date like '{0}%' and source = '45' and sent != '1'".format(date)


        # beyond30day_num = 0
        # sql_beyond30day = "select filename from  {0}".format(parse_tablename)
        # filenamelist = getDBdata(sql_beyond30day, '192.168.6.80', 'root', 'root', 'email_process')
        # for filename in filenamelist:
        #     filename = filename[0]
        #     print filename
        #     senttime = getSentTime(filename)
        #     flag = checkIfBeyond30Days(date, senttime)
        #     if flag == 1:
        #         beyond30day_num += 1
        # beyond30day_num = str(beyond30day_num)
        # print beyond30day_num


        parsenum = getDBdata(sql_parsenum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        fdfsnum = getDBdata(sql_fdfsnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        successnum = getDBdata(sql_successnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        failnum = getDBdata(sql_failnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        mongonum = getDBdata(sql_mongonum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        zl_succnum = getDBdata(sql_zlsuccnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        qc_succnum = getDBdata(sql_qcsuccnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        lp_succnum = getDBdata(sql_lpsuccnum, '192.168.6.80', 'root', 'root', 'email_process')[0][0]
        # enter_ycque_num = getDBdata(sql_ycquenum, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
        # yc_newadd_num = getDBdata(sql_ycnewaddnum, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]
        # yc_sendfail_num = getDBdata(sql_ycsendfailnum, '192.168.6.69', 'test', 'test', 'resume_update')[0][0]

        # insertIntoDB(date, zlnum, qcnum, lpnum, totalnum, parsenum, fdfsnum, successnum, failnum, mongonum, zl_succnum, qc_succnum, lp_succnum, enter_ycque_num, yc_newadd_num, yc_sendfail_num, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num, beyond30day_num)
        insertIntoDB(date, zlnum, qcnum, lpnum, totalnum, parsenum, fdfsnum, successnum, failnum, mongonum, zl_succnum, qc_succnum, lp_succnum, zl_savecol_num, zl_addcol_num, qc_savecol_num, qc_addcol_num, lp_savecol_num, lp_addcol_num)
        # writeToCSV([str(getTime()), zlnum, qcnum, lpnum, totalnum, emailinfo, parsenum, fdfsnum, successnum, failnum, mongonum], "email_info.csv")
        print 'end.'

