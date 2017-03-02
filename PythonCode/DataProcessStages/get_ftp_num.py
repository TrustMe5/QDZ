#coding=utf-8
import MySQLdb
import os,datetime
import sendmail
def getTodaydate():
    now_time = datetime.datetime.now()
    todaytime = now_time.strftime('%Y-%m-%d')
    return todaytime

def getCurrentTime():
    now_time = datetime.datetime.now()
    return now_time.strftime('%Y-%m-%d %H:%M:%S')

def writetofile(content):
    with open('/home/wudechao/Monitor_FTP_Num/task'+getTodaydate()+'.log','ab+') as wp:
        wp.write(content)
        wp.write("\n")
        wp.close()

def insertToDB(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute(sql)
    conn.commit()
    cur.close()

if __name__ == '__main__':
    cur_time = getCurrentTime()
    path = "/data/myftp"
    filenum = len(os.listdir(path))
    sql = "insert into 88_myftp_num(time, value) values('%s', '%s')" % (cur_time, str(filenum))
    insertToDB(sql, '10.18.99.179', 'root', 'root', 'monitor_info')
    if filenum > 20000:
        sendmail.send_mail(str(filenum))
        print '发送成功.'
    content = getCurrentTime() + " 99.88上myftp中的数量为: " + str(filenum)
    writetofile(content)
