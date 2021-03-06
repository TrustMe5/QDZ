#coding=utf-8
import sys, json
import urllib2
import sendmail, time
import MySQLdb, datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import logging, os
import psycopg2uo

user_agent = '"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36"'
headers = {'User-Agent': user_agent}

def initLogging(logname):
    fmt = '%(asctime)s %(name)s %(filename)s(%(funcName)s[line:%(lineno)d]) %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG,
                        format=fmt,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join('logs', '{}.log'.format(logname)),
                        filemode='a'
                        )
    console = logging.StreamHandler() 
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def getTime():
    now_time = datetime.datetime.now()
    return now_time.strftime('%Y-%m-%d %H:%M:%S')

def checkWebInfo(url, webcontent, exceptinfo):
    max_num = 10
    for i in range(max_num):
        try:
            req = urllib2.Request(url, headers = headers)
            resp = urllib2.urlopen(req)
            response = resp.read().decode('utf-8', 'ignore')
            if (response != webcontent):
                sendmail.send_mail(exceptinfo)
                logging.error("接口返回内容不等于" + webcontent)
                return 'N'
            else:
                return 'Y'
        except urllib2.URLError, e:
            if i < max_num - 1:
                continue
            else:
                logging.error(e.reason)
                logging.error(sys.exc_info())
                sendmail.send_mail(exceptinfo)
                return 'N'


def getDBdata(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    return data

def InsertIntoDB(current_time, contact_info, contact_dbinfo, fenjianli_webinfo, fenjianli_userdbinfo, fenjianli_searchinfo, fenjianli_uploadinfo, fenjianli_solrinfo, cvduo_solrinfo, fenjianli_resumedbinfo):
    conn = MySQLdb.connect(host="10.18.99.179", user="root", passwd="root", db="monitor_info", charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute("insert into  sendandfjl(time, contact_info, contact_dbinfo, fenjianli_webinfo, fenjianli_userdbinfo, fenjianli_searchinfo, fenjianli_uploadinfo, fenjianli_solrinfo, cvduo_solrinfo, fenjianli_resumedbinfo) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(current_time, contact_info, contact_dbinfo, fenjianli_webinfo, fenjianli_userdbinfo, fenjianli_searchinfo, fenjianli_uploadinfo, fenjianli_solrinfo, cvduo_solrinfo, fenjianli_resumedbinfo))
    conn.commit()
    cur.close()

def checkWebFenjianli(url, exceptinfo):             #检测纷简历网站是否正常访问
    max_num = 10
    for i in range(max_num):
        try:
            req = urllib2.Request(url, headers = headers)
            resp = urllib2.urlopen(req)
            htmlcontent = resp.read().decode('utf-8', 'ignore')
            if '关于纷简历' in htmlcontent:
                return 'Y'
            else:
                logging.error("纷简历网站内容错误！")
                sendmail.send_mail(exceptinfo)
                return 'N'
        except urllib2.URLError, e:
            if i < max_num - 1:
                continue
            else:
                logging.error(e.reason)
                logging.error(sys.exc_info())
                sendmail.send_mail(exceptinfo)
                return 'N'


def checkSolrInfo(url, exceptinfo):
    max_num = 10
    for i in range(max_num):
        try:
            req = urllib2.Request(url, headers = headers)
            resp = urllib2.urlopen(req)
            response = resp.read().decode('utf-8', 'ignore')
            json_data = json.loads(response)
            if json_data['response']['numFound'] != 0:
                return 'Y'
            else:
                logging.error(exceptinfo)
                sendmail.send_mail(exceptinfo)
                return 'N'
        except urllib2.URLError, e:
            if i < max_num - 1:
                continue
            else:
                logging.error(e.reason)
                logging.error(sys.exc_info())
                sendmail.send_mail(exceptinfo)
                return 'N'

def getDBdataFromPostgree(sql, host, user, passwd, db):
    conn = psycopg2.connect(database=db, user=user, password=passwd, host=host, port="5432")
    cur = conn.cursor()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    conn.close()
    return data




if __name__ == '__main__':
    while 1:
        initLogging("service")              #log暂时打印在service.log中
        logging.info("start...")
        current_time = getTime()
        ##############################################
        # 发送情况
        logging.info("准备检查请求联系方式的接口情况.")
        contact_info = checkWebInfo('http://192.168.6.80/getContact_DJ.php?', '{"status": "0", "errorMsg" : "ID不能为空"}', '请求联系方式的接口异常！')  #请求联系方式的接口情况
        logging.info("准备检查请求联系方式的数据库情况.")
        contact_dbinfo = 'Y'                #请求联系方式的数据库情况
        sql = "select count(*) from overview"
        try:
            row_num = getDBdata(sql, '192.168.6.69', 'test', 'test', 'resume_update')
            logging.info("请求联系方式的数据库情况正常.")
        except Exception, ex:
            logging.error(str(ex))
            logging.error(sys.exc_info())
            contact_dbinfo = 'N'
            sendmail.send_mail("请求联系方式的数据库部分异常！")
        ##############################################
        #纷简历情况请求
        logging.info("准备检查纷简历用户数据库的情况.")
        fenjianli_userdbinfo = 'Y'
        sql1 = "select count(*) from customer"
        try:
           row_num = getDBdata(sql1, '192.168.6.169', 'resume', 'zhimakaimen', 'resumedb')
           logging.info("纷简历用户数据库正常.")
        except Exception, ex:
            logging.error(str(ex))
            logging.error(sys.exc_info())
            fenjianli_userdbinfo = 'N'
            sendmail.send_mail("纷简历用户数据库异常！")

        logging.info("准备检查纷简历搜索服务器情况.")
        msg = '{"search resumes": "/resume/api/search", "get resumes list or content": {"url": "/resume/api/content", "explain": "type=list/con, default list", "extra parameters": "type"}}'
        fenjianli_searchinfo = checkWebInfo('http://192.168.6.61/resume/api', msg, '纷简历搜索服务器异常！')
        logging.info("准备检查纷简历网站情况.")
        fenjianli_webinfo = checkWebFenjianli('http://10.18.99.120', '纷简历网站不能访问！')
        logging.info("准备检查纷简历上传情况.")
        fenjianli_uploadinfo = checkWebInfo('http://upload.fenjianli.com/home/fileupload.htm', '{"result":0,"message":"请登录后重新上传!"}', '纷简历上传服务器异常！')
        logging.info("准备检查纷简历solr情况.")
        fenjianli_solrinfo = checkSolrInfo('http://10.18.99.92:8080/solr/resume_search/select?q=resume_id%3A45746485&wt=json&indent=true', "纷简历solr异常！")
        logging.info("准备检查cv多情况.")
        cvduo_solrinfo = checkSolrInfo('http://10.18.99.60:8080/solr/cvpt/select?q=resume_id%3A45746485&wt=json&indent=true', 'cv多solr异常！')
        logging.info("准备检查纷简历简历数据库情况.")
        fenjianli_resumedbinfo = 'Y'
        sql2 = "select count(*) from fjl.getmongoidtime"
        try:
           row_num = getDBdataFromPostgree(sql2, '192.168.6.120', 'postgres', '5432@pk_id', 'fjl_resume')
           logging.info("纷简历简历数据库正常.")
        except Exception, ex:
            logging.error(str(ex))
            logging.error(sys.exc_info())
            fenjianli_resumedbinfo = 'N'
            sendmail.send_mail("纷简历用户数据库异常！")

        print current_time, contact_info, contact_dbinfo, fenjianli_webinfo, fenjianli_userdbinfo, fenjianli_searchinfo, fenjianli_uploadinfo, fenjianli_solrinfo, cvduo_solrinfo, fenjianli_resumedbinfo

        logging.info("开始把服务器情况插入数据库..")
        try:
            # InsertIntoDB(current_time, contact_info, contact_dbinfo, fenjianli_webinfo, fenjianli_userdbinfo, fenjianli_searchinfo, fenjianli_uploadinfo, fenjianli_solrinfo, cvduo_solrinfo, fenjianli_resumedbinfo)
            logging.info("插入数据库完成..")
        except Exception, ex:
            logging.error(str(ex))
            logging.error(sys.exc_info())
            logging.info("插入数据库失败!")
        time.sleep(1800)
        # time.sleep(5)