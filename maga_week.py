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
import hashlib
from decimal import Decimal

d = datetime.now()
d_list = [1,2,3,4,5,6,7]
for i in d_list:
    delta = timedelta(days=i)
    weekday = d - delta
    if weekday.weekday() == 4:
        begin = weekday.strftime('%Y-%m-%d 00:00:00')
        continue
end = d.strftime('%Y-%m-%d %H:%M:%S')
print begin
print end


date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
md5id = hashlib.md5(str(d).encode('utf-8')).hexdigest()
pwd = os.getcwd()
folder = pwd + '/' + date
fuck_file = folder + '/' + date + '_kaohe.txt'
print folder
if not os.path.exists(folder):
    os.mkdir(folder)
db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
#db = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
#db = MySQLdb.connect("database_url","database_user","database_passwd","database_name")
cursor = db.cursor()
    #cursor.execute("select version()")
    #data = cursor.fetchone()
    #print "database version: %s" % data
if not os.path.exists(fuck_file):
    types = 'maga_week'
    db1 = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    cursor1 = db1.cursor()
    cursor1.execute("select * from sales_report where md5id='" + md5id + "' and types = '" + types + "'")
    data = cursor1.fetchall()
    if data:
        os._exit(0)
    #插入
    cursor1 = db1.cursor()
    sql = "insert into sales_report(md5id,types,begin,end) value('" + md5id + "','" + types + "','" + begin + "','" + end + "')"
    cursor1.execute(sql)
    db1.commit()
    
    #插入
    total = []
    ll = [['achou','CU'],['icy','AX'],['vava','XX']]
    for i in ll:
        i.append(md5id)
        total.append(i)
    print total
   
    cursor1 = db1.cursor()
    sql = "insert into magas_report_info(name,nick_name,md5id) value(%s,%s,%s)"
    cursor1.executemany(sql,total)
    db1.commit()

    #发现页面标签数量
    cursor1.execute("select p2.nick_name,p1.editor_id,count(p1.editor_id) as count from search_tags p1 left join users p2 on p2.user_id=p1.editor_id where p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p1.editor_id;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    print total
    sql = "update magas_report_info set tags_num = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()
    #立即可购数量    
    cursor1.execute("select p2.nick_name,p1.editor_id,count(p1.editor_id) as count from maga_cover p1 left join users p2 on p2.user_id=p1.editor_id where p1.skey='0' and p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p1.editor_id;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    sql = "update magas_report_info set lijikegou_num = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()
    #普通杂志
    cursor1.execute("select p2.nick_name,p1.editor_id,count(p1.editor_id) as count from maga_cover p1 left join users p2 on p2.user_id=p1.editor_id where p1.skey='1' and p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p1.editor_id;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    sql = "update magas_report_info set magazine_num = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()
    #db1.close()

    
    

    #标签浏览数
    cursor1.execute("select p2.nick_name,p1.editor_id,sum(p1.count) as count from search_tags p1 left join users p2 on p2.user_id=p1.editor_id where p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p2.nick_name;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    sql = "update magas_report_info set tags_view = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()



    #立即可购浏览数
    cursor1.execute("select p2.nick_name,p1.editor_id,sum(p1.count) as count from maga_cover p1 left join users p2 on p2.user_id=p1.editor_id where p1.skey='0' and p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p2.nick_name;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    sql = "update magas_report_info set lijikegou_view = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()

    #普通杂志浏览数    
    cursor1.execute("select p2.nick_name,p1.editor_id,sum(p1.count) as count from maga_cover p1 left join users p2 on p2.user_id=p1.editor_id where p1.skey='1' and p1.publish_time > '" + begin + "' and p1.publish_time < '" + end + "' group by p2.nick_name;")
    data = cursor1.fetchall()
    total = []
    for row in data:
        rb = []
        if row['nick_name'] == 'CU':
            name = 'achou'
        elif row['nick_name'] == 'AX':
            name = 'icy'
        elif row['nick_name'] == 'XX':
            name = 'vava'
        else:
            name = 'other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(name)
        total.append(rb)
    sql = "update magas_report_info set magazine_view = %s where md5id = %s and name = %s limit 1"
    cursor1.executemany(sql,total)
    db1.commit()

    os._exit(0)
    #商品浏览数
    cursor.execute("select p2.attribute_set_id,count(p2.attribute_set_id) as count from report_viewed_product_index p1 left join catalog_product_entity p2 on p2.entity_id = p1.product_id where p1.added_at > '" + begin + "' and p1.added_at < '" + end + "' group by p2.attribute_set_id;")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(row['attribute_set_id'])
        total.append(rb)
    sql = "update magas_report_info set product_view = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()




os._exit(0)
if os.path.exists(fuck_file):
    file = codecs.open(fuck_file, 'r', encoding='utf-8')
    res = file.read()
    res = json.loads(res)
    rq = []
    for i in res:
        rb = []
        fuck_count = res[i] * 0.3
        rb.append(fuck_count)
        rb.append(i)
        rq.append(rb)
    sql = "update table_auto_order set fuck_count = %s where entity_id = %s"
    cursor.executemany(sql,rq)
    db.commit()

    sql2 = "update catalog_product_entity,table_auto_order set catalog_product_entity.created_at = table_auto_order.updated_at where catalog_product_entity.entity_id = table_auto_order.entity_id"
    cursor.execute(sql2)
    do.commit()
db.close()
