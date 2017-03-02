#coding=utf-8
import smtplib
from email.mime.text import MIMEText


mailto_list = ["wudechao@qiaodata.com"]
mail_host = "smtp.163.com"  # 设置服务器
mail_user = "18369959235"  # 用户名
mail_pass = "wdc930122"  # 口令
mail_postfix = "163.com"  # 发件箱的后缀


def send_mail(time1,time2,num1,num2):
    # with open('monitor_', 'r') as rf:
    #     content=rf.read()
    content = '自然人表中从%s到%s的数量：%s'%(time1,time2,num1) +'\n纷简历表中从%s到%s的数量：%s'%(time1,time2,num2)
    to_list=mailto_list
    sub='Monitor_FTP_Num'
    me = "wudechao" + "<" + mail_user + "@" + mail_postfix + ">"

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
    if send_mail():
        print 'ok'
    else:
        print 'error'