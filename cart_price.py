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
import decimal

date = datetime.now()
date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
pwd = os.getcwd()
folder = pwd + '/' + date
cart_file = folder + '/' + date + '_cart_price.txt'
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
    #cursor.execute("select ps.sku from sales_flat_quote pe left join sales_flat_quote_item ps on ps.quote_id=pe.entity_id where pe.is_active = '1' and ps.parent_item_id is null and ps.product_id is not null")
    cursor.execute("select ps.sku,p1.value as price,p2.value as special_price from sales_flat_quote pe left join sales_flat_quote_item ps on ps.quote_id=pe.entity_id left join catalog_product_entity_decimal p1 on ps.product_id = p1.entity_id and p1.attribute_id='64' left join catalog_product_entity_decimal p2 on p2.entity_id = ps.product_id and p2.attribute_id='65' where pe.is_active = '1' and ps.parent_item_id is null and ps.product_id is not null")

    data = cursor.fetchall()
    r_list = []
    b_list = []
    for row in data:
        r_list.append(row['sku'][0:8])
        a_list = []
        a_list.append(row['sku'][0:8])
        a_list.append(str(row['price']))
        a_list.append(str(row['special_price']))
        b_list.append(a_list)
    #db.close()

    #print r_list
    res = Counter(r_list)
    res = json.loads(json.dumps(dict(res), ensure_ascii=False))
    print res
   
    for i in b_list:
        for j in res:
            if j == i[0]:
                i.append(res[j])
                #print i[0]
                #print i[1]
    print b_list
    line = json.dumps(b_list) + "\n"
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
    print res
    rq = []
    for i in res:
        rb = []
        print i
        if i[2] == 'None':
            cart_price = i[3] * 0.001 * float(i[1])
        else:
            cart_price = i[3] * 0.001 * float(i[2])
        rb.append(cart_price)
        rb.append(i[0])
        rq.append(rb)
    print rq
    sql = "update table_auto_order set cart_price = %s where sku = %s"
    cursor.executemany(sql,rq)
    db.commit()
db.close()
