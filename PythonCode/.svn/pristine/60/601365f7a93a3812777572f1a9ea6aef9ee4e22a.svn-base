#coding=utf-8
import os
from sendmail import send_mail
if __name__ == '__main__':
    path = "/data/myftp"
    filenum = len(os.listdir(path))
    if filenum >50000:
        send_mail(filenum)