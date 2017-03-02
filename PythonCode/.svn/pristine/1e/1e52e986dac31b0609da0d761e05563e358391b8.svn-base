#coding=utf-8
import smtplib
import poplib
from email.mime.text import MIMEText

def send_mail(filenum):
    mailto_list = ["wudechao@qiaodata.com"]
    mail_host = "smtp.qiaodata.com"  # 设置服务器
    mail_user = "monitor@qiaodata.com"  # 用户名
    mail_pass = "Wdc930122"  # 口令
    mail_postfix = "qiaodata.com"  # 发件箱的后缀

    # with open('monitor_', 'r') as rf:
    #     content=rf.read()
    content = "当前coll_upload文件数目为：%s，异常请及时处理。" % filenum
    to_list = mailto_list
    sub='coll_upload异常'
    me = "monitor" + "<"  + "monitor@" + mail_postfix + ">"
    print me
    msg = MIMEText(content, _subtype='plain', _charset='utf-8')
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(me, to_list, msg.as_string())
        server.close()
        return True
    except Exception, e:
        print str(e)
        return False

if __name__=='__main__':
    print 'hello'
    if send_mail("20"):
        print 'ok'
    else:
        print 'error'
    print 'end.'