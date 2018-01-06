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
import random

now = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
#nows =  now.strftime('%Y-%m-%d %H:%M:%S')
#print nows
delta = timedelta(days=30)
n_days = now - delta
n_days =  n_days.strftime('%Y-%m-%d %H:%M:%S')
print n_days

#date = datetime.now()
date = time.strftime('%Y-%m-%d',time.localtime(time.time()))

print date


pwd = os.getcwd()
folder = pwd + '/' + date
fuck_file = folder + '/' + date + '_created_at_sort.txt'
print folder
if not os.path.exists(folder):
    os.mkdir(folder)
db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()
    #cursor.execute("select version()")
    #data = cursor.fetchone()
    #print "database version: %s" % data
    
if os.path.exists(fuck_file):    
    os.remove(fuck_file)
if not os.path.exists(fuck_file):
    #cursor.execute("select ps.sku from sales_flat_quote pe left join sales_flat_quote_item ps on ps.quote_id=pe.entity_id where pe.is_active = '1' and ps.parent_item_id is null and ps.product_id is not null limit 10")
    #cursor.execute("select max(entity_id) as max,min(entity_id) as min from table_auto_order where created_at > %s",(n_days))
    #result = cursor.fetchone()
    #max_id = result['max']
    #min_id = result['min']
    #cursor.execute("select entity_id,attribute_set_id from table_auto_order where created_at > %s order by entity_id desc limit 0,200",(n_days))
    cursor.execute("select entity_id,attribute_set_id from table_auto_order order by entity_id desc limit 0,500")
    data1 = cursor.fetchall()

    #cursor.execute("select entity_id,attribute_set_id from table_auto_order where created_at > %s order by entity_id desc limit 200,5000",(n_days))
    cursor.execute("select entity_id,attribute_set_id from table_auto_order order by entity_id desc limit 500,8000")
    data2 = cursor.fetchall()
    
    ########################begin data1###############################
    a_list = []
    b_list = []
    c_list = []
    d_list = []
    e_list = []
    f_list = []
    for row in data1:
        if row['attribute_set_id'] == 10:
            a_list.append(row['entity_id'])
        if row['attribute_set_id'] == 12:
            b_list.append(row['entity_id'])
        if row['attribute_set_id'] == 13:
            c_list.append(row['entity_id'])
        if row['attribute_set_id'] == 16:
            d_list.append(row['entity_id'])
        if row['attribute_set_id'] == 15:
            e_list.append(row['entity_id'])
        if row['attribute_set_id'] == 11:
            f_list.append(row['entity_id'])
    random.shuffle(a_list)
    random.shuffle(b_list)
    random.shuffle(c_list)
    random.shuffle(d_list)
    random.shuffle(e_list)
    random.shuffle(f_list)
    total_list = []
    for index,val in enumerate(data1):
        if a_list:
            total_list.extend(a_list[0:4])
            for i in range(4):
                if a_list:
                    a_list.pop(0)
        if (index % 2) == 0:
            if b_list:
                total_list.extend(b_list[0:2])
                for i in range(2):
                    if b_list:
                        b_list.pop(0)
        else:
            if c_list:
                total_list.extend(c_list[0:2])
                for i in range(2):
                    if c_list:
                        c_list.pop(0)
        if d_list:
            total_list.extend(d_list[0:4])
            for i in range(4):
                if d_list:
                    d_list.pop(0)
        if e_list:
            total_list.extend(e_list[0:2])
            for i in range(2):
                if e_list:
                    e_list.pop(0)
        if f_list:
            total_list.extend(f_list[0:2])
            for i in range(2):
                if f_list:
                    f_list.pop(0)
    #print total_list
    #########################begin data2###################################
    a_list = []
    b_list = []
    c_list = []
    d_list = []
    e_list = []
    f_list = []
    for row in data2:
        if row['attribute_set_id'] == 10:
            a_list.append(row['entity_id'])
        if row['attribute_set_id'] == 12:
            b_list.append(row['entity_id'])
        if row['attribute_set_id'] == 13:
            c_list.append(row['entity_id'])
        if row['attribute_set_id'] == 16:
            d_list.append(row['entity_id'])
        if row['attribute_set_id'] == 15:
            e_list.append(row['entity_id'])
        if row['attribute_set_id'] == 11:
            f_list.append(row['entity_id'])
    random.shuffle(a_list)
    random.shuffle(b_list)
    random.shuffle(c_list)
    random.shuffle(d_list)
    random.shuffle(e_list)
    random.shuffle(f_list)
    total_list2 = []
    for index,val in enumerate(data2):
        if a_list:
            total_list2.extend(a_list[0:4])
            for i in range(4):
                if a_list:
                    a_list.pop(0)
        if (index % 2) == 0:
            if b_list:
                total_list2.extend(b_list[0:2])
                for i in range(2):
                    if b_list:
                        b_list.pop(0)
        else:
            if c_list:
                total_list2.extend(c_list[0:2])
                for i in range(2):
                    if c_list:
                        c_list.pop(0)
        if d_list:
            total_list2.extend(d_list[0:4])
            for i in range(4):
                if d_list:
                    d_list.pop(0)
        if e_list:
            total_list2.extend(e_list[0:2])
            for i in range(2):
                if e_list:
                    e_list.pop(0)
        if f_list:
            total_list2.extend(f_list[0:2])
            for i in range(2):
                if f_list:
                    f_list.pop(0)

    total = total_list + total_list2
    #print total_list
    #print "+++++++++++++++++++++++++++++++++++++++"
    #print total_list2    

    total_lists = []
    now = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
    #nows =  now.strftime('%Y-%m-%d %H:%M:%S')
    for index,val in enumerate(total):
        rb = []
        delta = timedelta(seconds=index)
        n_days = now - delta
        n_days =  n_days.strftime('%Y-%m-%d %H:%M:%S')

        rb.append(n_days)
        rb.append(val)
        total_lists.append(rb)
   
    line = json.dumps(total_lists) + "\n"
    file = codecs.open(fuck_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    print "1"

    sql = "update table_auto_order set created_at_sort = %s where entity_id = %s"
    cursor.executemany(sql,total_lists)
    db.commit()

    #sql2 = "update catalog_product_entity,table_auto_order set catalog_product_entity.created_at = table_auto_order.updated_at where catalog_product_entity.entity_id = table_auto_order.entity_id"
    #cursor.execute(sql2)
    #db.commit()

db.close()


os._exit(0)
print "=========="
print fuck_file
if os.path.exists(fuck_file):
    file = codecs.open(fuck_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    #print res
    rq = []
    for i in res:
        rb = []
        fuck_count = res[i] * 0.3
        rb.append(fuck_count)
        rb.append(i)
        rq.append(rb)
    #print rq
    sql = "update table_auto_order set fuck_count = %s where entity_id = %s"
    cursor.executemany(sql,rq)
    db.commit()

    sql2 = "update catalog_product_entity,table_auto_order set catalog_product_entity.created_at = table_auto_order.updated_at where catalog_product_entity.entity_id = table_auto_order.entity_id"
    cursor.execute(sql2)
    do.commit()
db.close()
