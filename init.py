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
entity_file = folder + '/' + date + '_entity_id.txt'
if not os.path.exists(folder):
    os.mkdir(folder)
#if not os.path.exists(entity_file):
db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
#db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()
if not os.path.exists(entity_file):
    #cursor.execute("select p1.entity_id,p1.sku,p1.attribute_set_id,p1.final_created_at from catalog_product_entity p1 left join catalog_product_entity_int p2 on p1.entity_id=p2.entity_id and p2.attribute_id='84' left join catalog_product_entity_int p3 on p1.entity_id=p3.entity_id and p3.attribute_id='91' where p2.value='1' and p3.value='4' and p1.entity_id !='6705'")

    cursor.execute("select p1.entity_id,p1.sku,p1.attribute_set_id,p1.final_created_at from catalog_product_entity p1 left join catalog_product_entity_int p2 on p1.entity_id=p2.entity_id and p2.attribute_id='84' left join catalog_product_entity_int p3 on p1.entity_id=p3.entity_id and p3.attribute_id='91' left join cataloginventory_stock_status p4 on p1.entity_id=p4.product_id where p2.value='1' and p3.value='4' and p4.stock_status = '1' and p1.entity_id != '6705'")
    data = cursor.fetchall()
    #print data 
    #db.close()

    line = json.dumps(data, cls=CJsonEncoder) + "\n"
    file = codecs.open(entity_file, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    print "1"

print "=========="
print entity_file


if os.path.exists(entity_file):
    file = codecs.open(entity_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    #print res    
    sql = "truncate table_auto_order"
    cursor.execute(sql)
    db.commit()

    rq = []
    for i in res:
        rb = []
        rb.append(i['entity_id'])
        rb.append(i['sku'])
        rb.append(i['attribute_set_id'])
        rb.append(i['final_created_at'])
        rq.append(rb)
    #print rq

    sql="insert into table_auto_order(entity_id,sku,attribute_set_id,created_at) values(%s,%s,%s,%s)"
    cursor.executemany(sql,rq)
    db.commit()
db.close()
