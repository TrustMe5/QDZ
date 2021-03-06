# -*- coding: utf-8 -*-
#import gevent.monkey
#gevent.monkey.patch_all()
#gevent.monkey.patch_all(socket=False, thread=False)
import sys
import gevent
import datetime
from redis import ConnectionPool, Redis
import logging
import os
import time
from multiprocessing import freeze_support, cpu_count, Pool, current_process
import shutil


def initLogging(logname):
    fmt = '%(asctime)s %(name)s %(filename)s(%(funcName)s[line:%(lineno)d]) %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG,
                        format=fmt,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join('logs', '{}.{}.log'.format(logname, time.strftime('%Y%m%d%H'))),
                        filemode='a'
                        )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def redis_connect():
    host = '192.168.6.174'
    port = 6379
    pool_count = 1000
    try:
        pool = ConnectionPool(max_connections=pool_count, host=host,
                              port=port)
        conn = Redis(connection_pool=pool)
    except:
        logging.error("连接redis时失败.")
        return False
    return conn


tm = lambda: datetime.datetime.now().strftime("%Y-%m-%d %H:%S:%M")


def check_del_file(root):
    dir_name = os.path.split(root)[1]
    now_dir_name = datetime.datetime.now().strftime("%Y%m%d")
    try:
        return dir_name[0:8] < now_dir_name
    except Exception, ex:
        logging.error(str(ex))
    return False


def move_file(in_path, out_file):

    try:
        t = time.time()
        shutil.move(in_path, target_dir)
        print "->fjl:", time.time() - t
        logging.info("移动 {}".format(in_path))
        print "MOVE {}".format(in_path)
        return True
    except Exception, ex:
        logging.error("FJL file error %r" % ex)
        if 'already exists' in str(ex):
            try:
                os.remove(in_path)
            except:
                logging.error("删除 {} 时失败.".format(in_path))
    return False


def worker(in_path, out_file):
    try:
        out_path = target_dir + os.sep + out_file
        res = move_file(in_path, out_file)
        msg = "{}->({})".format(in_path,  out_path)
        logging.info(msg)
    except Exception, ex:
        msg = "{} MOVE FAIL:{} {}".format(tm(), in_path, ex)
        logging.error(msg)



def do_files(root, files):
    print 'Starting  PID:%s ' % os.getpid()
    logging.info('Starting  PID:%s ' % os.getpid())
    if files and isinstance(files, list):
        for f in files:
            if f and writing_flag not in f:
                worker(root + os.sep + f, f)

def main():
    global per_num
    try:
        while True:
            initLogging('fjl')
            print "%s waiting。。。" % datetime.datetime.now()
            list_dirs = [d for d in os.listdir(source_dir) if d not in ['unzip2151103', 'unzip_bak', 'error', 'Flume']]
            logging.info("当前目录个数：{}".format(len(list_dirs)))
            opnum = 0
            stm = time.time()
            for list_dir in list_dirs:
                list_dir = os.path.join(source_dir, list_dir)
                try:
                    for root, dirs, files in os.walk(list_dir):
                        if files:
                            opnum = len(files)
                            if len(files) < (per_num * cpu_num):
                                per_num = len(files)/cpu_num
                                per_num = per_num if per_num else 1

                            logging.info("目录 {} 中的文件数目：{}".format(dirs, len(files)))
                            for i in range(0, len(files), per_num):
                                do_files(root, files[i: i + per_num])
                    logging.info("进程池关闭.")

                    # 删除过时空目录
                    if not os.listdir(list_dir) and check_del_file(list_dir):
                        try:
                            os.rmdir(list_dir)
                            print "move empty dir: {}".format(list_dir)
                            logging.info("删除目录：{}".format(list_dir))
                        except:
                            logging.error("删除过时空目录 {} 时失败！".format(list_dir))
                except Exception, ex:
                    logging.error(str(ex))

            print "Files num:{} with time：{}".format(opnum, time.time() - stm)
            logging.info("Files num:{} with time：{}".format(opnum, time.time() - stm))
            cc = int(sys.argv[1]) if len(sys.argv) > 1 else 5
            for i in xrange(cc):
                print str(i) + os.linesep if i >= (cc - 1) else i,
                sys.stdout.flush()
                time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print "Stop"
        logging.error("主程序退出.")


if __name__ == "__main__":
    initLogging('fjl')
    abs_path = '/'
    queue_name = 'rftest'
    source_dir = os.path.join(abs_path, 'TMD2')
    err_dir = os.path.join(abs_path, 'TMD2', 'error')
    target_dir = os.path.join(abs_path, 'rData2')
    writing_flag = 'writing'
    per_num = 1000
    cpu_num = 1  # cpu_count()
    main()
