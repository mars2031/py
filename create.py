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
cart_file = folder + '/' + date + '_create.txt'
if not os.path.exists(folder):
    os.mkdir(folder)
if not os.path.exists(cart_file):
    db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
    cursor = db.cursor()
    cursor.execute("select sku,final_created_at from catalog_product_entity limit 1000")
    data = cursor.fetchall()
    print data 
    db.close()

    line = json.dumps(data, cls=CJsonEncoder) + "\n"
    file = codecs.open(cart_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    print "1"

print "=========="
print cart_file

#date_now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
now = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
print now
delta = timedelta(days=7)
print delta

n_days = now - delta
print n_days.strftime('%Y-%m-%d %H:%M:%S')


if os.path.exists(cart_file):
    file = codecs.open(cart_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    print res    
    now = datetime.now()
    delta = timedelta(days=7)
    n_days = now - delta

    delta1 = timedelta(days=14)
    n_days1 = now - delta1

    delta2 = timedelta(days=21)
    n_days2 = now - delta2

    delta3 = timedelta(days=28)
    n_days3 = now - delta3


    for i in res:
        r = datetime.strptime(i['final_created_at'],'%Y-%m-%d %H:%M:%S')
        if r < now and r > n_days:
            i['num'] = 6
        elif r < n_days and r > n_days1:
            i['num'] = 3
        elif r < n_days1 and r > n_days2:
            i['num'] = 1.25
        elif r < n_days2:
            i['num'] = 0.75
    print res 
    line = json.dumps(res, cls=CJsonEncoder) + "\n"
    file = codecs.open(cart_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
