#coding=utf-8

import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def getDBdataFrom81(sql, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    oper = cur.execute(sql)
    data = cur.fetchmany(oper)
    cur.close()
    return data

def insertDataToDB(sql_insert, host, user, passwd, db):
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8')
    cur = conn.cursor()
    cur.execute('SET NAMES UTF8')
    conn.commit()
    cur.execute(sql_insert)
    conn.commit()
    cur.close()

if __name__ == '__main__':
    sql = "select from_id, from_salary_name, from_salary_id from hr_relation_salary"
    datalist = getDBdataFrom81(sql, "192.168.6.81","miaohr","miaohr1qaz","hr_dict_center")
    for data in datalist:
        from_id = data[0]
        from_work_year_name = data[1]
        from_work_year_id = data[2]
        print from_id, from_work_year_name, from_work_year_id
        try:
            sql_insert = "insert into salary_channel_copy(sid,name,code) values('%s','%s','%s')" % (from_id, from_work_year_name, from_work_year_id)
            insertDataToDB(sql_insert, "192.168.6.81", "miaohr", "miaohr1qaz", "dict")
        except:
            print "data is exists."
