#coding=utf-8
import smtplib
import poplib
from email.mime.text import MIMEText

def send_mail(info):
    # mailto_list = ["zhuzheng@qiaodata.com", "pangjianli@qiaodata.com", "wudechao@qiaodata.com"]
    mailto_list = ["wudechao@qiaodazhao.com"]
    mail_host = "smtp.qiaodata.com"  # 设置服务器
    mail_user = "monitor@qiaodata.com"  # 用户名
    mail_pass = "Wdc930122"  # 口令
    mail_postfix = "qiaodata.com"  # 发件箱的后缀

    # with open('monitor_', 'r') as rf:
    #     content=rf.read()
    content = info
    to_list = mailto_list
    sub = info
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
    if send_mail(""):
        print 'ok'
    else:
        print 'error'
    print 'end.'