#coding=utf-8
import urllib2, MySQLdb
from bs4 import BeautifulSoup
import sys,datetime,time
import sendmail
reload(sys)
sys.setdefaultencoding('utf8')

def getHtmlInfo(url):
    num = 0
    req = urllib2.Request(url)
    try:
        resp = urllib2.urlopen(req)
    except urllib2.HTTPError, error:
        print "Cannot remove service instance!", error
        sys.exit(1)
    soup = BeautifulSoup(resp.read(),"html.parser")

    td_list = soup.find_all('a',attrs={'href':'?view&s=2&key=coll_upload'})
    if len(td_list) > 0 and td_list[0].find('span'):
       num = td_list[0].find('span').get_text().replace('(','').replace(')','')
    return num

def getTodaydate():
    now_time = datetime.datetime.now()
    todaytime = now_time.strftime('%Y-%m-%d')
    return todaytime

def getCurrentTime():
    now_time = datetime.datetime.now()
    return now_time.strftime('%Y-%m-%d %H:%M:%S')

def writetofile(content):
    with open('/home/wudechao/Monitor_FastdfsNum/task'+getTodaydate()+'.log','ab+') as wp:
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
    url = "http://192.168.6.69/phpRedisAdmin/?view&s=2&key=coll_upload"
    num = getHtmlInfo(url)

    cur_time = getCurrentTime()
    sql = "insert into fdfs_num(time, value) values('%s', '%s')" % (cur_time, num)
    insertToDB(sql, '10.18.99.179', 'root', 'root', 'monitor_info')

    if int(num) and int(num) >= 30000:
        sendmail.send_mail(str(num))
    content = getCurrentTime() + " coll_upload中的数量为: " + str(num)
    writetofile(content)
