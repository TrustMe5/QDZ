#coding=utf-8
import urllib2
from bs4 import BeautifulSoup
import sys
from sendmail import send_mail
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



if __name__ == '__main__':
    url = "http://192.168.6.69/phpRedisAdmin/?view&s=2&key=coll_upload"
    num = getHtmlInfo(url)
    if int(num) and int(num) > 50000:
        send_mail(num)
