#!/usr/bin/python
# -*- coding:UTF-8 -*-

import MySQLdb
import MySQLdb.cursors
from collections import Counter
import os
from datetime import datetime
import time
import codecs
import json


date = datetime.now()
date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
pwd = os.getcwd()
folder = pwd + '/' + date
cart_file = folder + '/' + date + '_cart.txt'
print folder
if not os.path.exists(folder):
    os.mkdir(folder)
db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()
    #cursor.execute("select version()")
    #data = cursor.fetchone()
    #print "database version: %s" % data
if not os.path.exists(cart_file):
    cursor.execute("select ps.sku from sales_flat_quote pe left join sales_flat_quote_item ps on ps.quote_id=pe.entity_id where pe.is_active = '1' and ps.parent_item_id is null and ps.product_id is not null")
    data = cursor.fetchall()
    r_list = []
    for row in data:
        r_list.append(row['sku'][0:8])
    #db.close()

    #print r_list
    res = Counter(r_list)
    #print dict(res)

    line = json.dumps(dict(res), ensure_ascii=False) + "\n"
    file = codecs.open(cart_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    print "1"

print "=========="
print cart_file
if os.path.exists(cart_file):
    file = codecs.open(cart_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    #print res
    rq = []
    for i in res:
        rb = []
        cart_count = res[i] * 2#0.5
        rb.append(cart_count)
        rb.append(i)
        rq.append(rb)
    #print rq
    sql = "update table_auto_order set cart_count = %s where sku = %s"
    cursor.executemany(sql,rq)
    db.commit()
db.close()
