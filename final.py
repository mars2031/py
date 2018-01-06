#!/usr/bin/python
# -*- coding:UTF-8 -*-

import MySQLdb
import MySQLdb.cursors
from collections import Counter
import os
from datetime import datetime
from datetime import timedelta
import time
import codecs
import json

class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def addWord(theIndex,word,pagenumber):
    theIndex.setdefault(word,[ ]).append(pagenumber)

date = datetime.now()
date = time.strftime('%Y-%m-%d',time.localtime(time.time()))



pwd = os.getcwd()
folder = pwd + '/' + date
final_file = folder + '/' + date + '_final.txt'
if not os.path.exists(folder):
    os.mkdir(folder)

db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
#db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()

if not os.path.exists(final_file):
    #cursor.execute("select sku,final_created_at from catalog_product_entity limit 1000")
    cursor.execute("select * from table_auto_order order by week_num desc")
    data = cursor.fetchall()
    #print data 
    #db.close()

    line = json.dumps(data, cls=CJsonEncoder) + "\n"
    file = codecs.open(final_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()

print "=========="
print final_file

#date_now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
now = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
print now
delta = timedelta(days=7)
print delta

n_days = now - delta
print n_days.strftime('%Y-%m-%d %H:%M:%S')


if os.path.exists(final_file):
    file = codecs.open(final_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    #print res    
    now = datetime.now()

    delta = timedelta(days=2)
    n_days0 = now - delta

    delta = timedelta(days=7)
    n_days = now - delta

    delta1 = timedelta(days=14)
    n_days1 = now - delta1

    delta2 = timedelta(days=21)
    n_days2 = now - delta2

    delta3 = timedelta(days=28)
    n_days3 = now - delta3

    delta4 = timedelta(days=35)
    n_days4 = now - delta4

    list1 = [1,2,3,4,5]
    list2 = [1,2,3,4,5,6,7]
    for i in res:
        r = datetime.strptime(i['created_at'],'%Y-%m-%d %H:%M:%S')
        if r < now and r > n_days0:
            i['num'] = 57.4
        elif r < n_days0 and r > n_days:
            for j in list1:
                day_date = timedelta(days=j)
                date1 = n_days0 - day_date
                if date1 < r < n_days0:
                    #i['num'] = 60 - (4 * int(j))
                    i['num'] = 57.4
                    print date1.strftime('%Y-%m-%d %H:%M:%S') + "<" + r.strftime('%Y-%m-%d %H:%M:%S') + "<" + n_days0.strftime('%Y-%m-%d %H:%M:%S') + "=" + str(i['num']) + "= 65 - ( 4 * " + str(j)
                    break
                    #(65 - 45)/5 = 4
        elif r < n_days and r > n_days1:
            for j in list2:
                day_date = timedelta(days=j)
                date1 = n_days - day_date
                if date1 < r < n_days:
                    #i['num'] = 45 - (3 * int(j))
                    i['num'] = 53.3
                    print date1.strftime('%Y-%m-%d %H:%M:%S') + "<" + r.strftime('%Y-%m-%d %H:%M:%S') + "<" + n_days.strftime('%Y-%m-%d %H:%M:%S') + "=" + str(i['num']) + "= 45 - ( 3 * " + str(j)
                    break
        elif r < n_days1 and r > n_days2:
            for j in list2:
                day_date = timedelta(days=j)
                date1 = n_days1 - day_date
                if date1 < r < n_days1:
                    #i['num'] = 30 - (2 * int(j))
                    i['num'] = 53.3
                    print date1.strftime('%Y-%m-%d %H:%M:%S') + "<" + r.strftime('%Y-%m-%d %H:%M:%S') + "<" + n_days1.strftime('%Y-%m-%d %H:%M:%S') + "=" + str(i['num']) + "= 30 - ( 2 * " + str(j)
                    break
        elif r < n_days2 and r > n_days3:
            for j in list2:
                day_date = timedelta(days=j)
                date1 = n_days2 - day_date
                if date1 < r < n_days2:
                    #i['num'] = 20 - (2 * int(j))
                    i['num'] = 49.2
                    print date1.strftime('%Y-%m-%d %H:%M:%S') + "<" + r.strftime('%Y-%m-%d %H:%M:%S') + "<" + n_days2.strftime('%Y-%m-%d %H:%M:%S') + "=" + str(i['num']) + "= 20 - ( 2 * " + str(j)
                    break
        elif r < n_days3:
            #i['num'] = 5
            i['num'] = 16.4
        #i['num'] = 45 - (( 45 - 20)/5) |   当周权重 - ((当周权重 - 下周权重)/5)
        #65 45 30 20 5
    #print res 
    line = json.dumps(res, cls=CJsonEncoder) + "\n"
    file = codecs.open(final_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    rb = []
    for i in res:
        rq = []
        effect = i['cart_count'] + i['order_count'] + i['wishlist_count'] + i['view_count'] + i['cart_price']
        value = effect * 0.18 + i['num']
        rq.append(effect)
        rq.append(i['num'])
        rq.append(value)
        rq.append(i['entity_id'])
        rb.append(rq)
    #print rb

    sql = "update table_auto_order set effect = %s,week_num = %s,value = %s where entity_id = %s"
    cursor.executemany(sql,rb)
    db.commit()

    #print rb

db.close()
