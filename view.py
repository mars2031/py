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
view_file = folder + '/' + date + '_view.txt'
print folder
if not os.path.exists(folder):
    os.mkdir(folder)
db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()
    #cursor.execute("select version()")
    #data = cursor.fetchone()
    #print "database version: %s" % data
if not os.path.exists(view_file):
    #cursor.execute("select ps.sku from sales_flat_quote pe left join sales_flat_quote_item ps on ps.quote_id=pe.entity_id where pe.is_active = '1' and ps.parent_item_id is null and ps.product_id is not null limit 10")
    #cursor.execute("select product_id from report_viewed_product_index where visitor_id is NULL")
    cursor.execute("select product_id from report_viewed_product_index p1 left join catalog_product_entity_int p2 on p1.product_id=p2.entity_id and p2.attribute_id='84' left join catalog_product_entity_int p3 on p1.product_id=p3.entity_id and p3.attribute_id='91' where p2.value='1' and p3.value='4' and p1.visitor_id is NULL")
    data = cursor.fetchall()
    r_list = []
    for row in data:
        #r_list.append(row['sku'][0:8])
        r_list.append(row['product_id'])
    #db.close()

    #print r_list
    res = Counter(r_list)
    #print dict(res)

    line = json.dumps(dict(res), ensure_ascii=False) + "\n"
    file = codecs.open(view_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    print "1"

print "=========="
print view_file
if os.path.exists(view_file):
    file = codecs.open(view_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    #print res
    rq = []
    for i in res:
        rb = []
        count = res[i] * 0.3#0.05
        if count > 150:
            view_count = 150
        else:
            view_count = count
        rb.append(view_count)
        rb.append(i)
        rq.append(rb)
    #print rq
    sql = "update table_auto_order set view_count = %s where entity_id = %s"
    cursor.executemany(sql,rq)
    db.commit()
db.close()
