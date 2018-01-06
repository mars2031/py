#!/usr/bin/python
# -*- coding:UTF-8 -*-

import MySQLdb
import MySQLdb.cursors
from collections import Counter
import os
import datetime
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
    if weekday.weekday() == 3:
        
        end = weekday.strftime('%Y-%m-%d 23:59:59')
        delta = timedelta(days=28)
        begin = weekday - delta
        begin = begin.strftime('%Y-%m-%d 00:00:00')
        continue

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
#data cursor.fetchone()
    #print "database version: %s" % data
if not os.path.exists(fuck_file):
    types = 'product_month'
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

    cursor.execute("select p1.attribute_set_id,count(p1.attribute_set_id) as count from catalog_product_entity p1 left join catalog_product_entity_int p2 on p1.entity_id=p2.entity_id and p2.attribute_id='91' left join catalog_product_entity_int p3 on p3.entity_id=p1.entity_id and p3.attribute_id='84' where p1.final_created_at > '" + begin + "' and p1.final_created_at < '" + end + "' and p2.value = '4' and p3.value = '1' group by p1.attribute_set_id")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        #achou=10服装 | icy=12鞋子 16配饰 11美妆 | vava= 13包 15内衣
        if row['attribute_set_id'] == 10:
            name = 'achou'
        elif row['attribute_set_id'] == 12 or row['attribute_set_id'] == 16 or row['attribute_set_id'] == 11:
            name = 'icy'
        elif row['attribute_set_id'] == 13 or row['attribute_set_id'] == 15:
            name = 'vava'
        else:
            name = 'other'
        
        if row['attribute_set_id'] == 10:
            category_name = 'Clothers'
        elif row['attribute_set_id'] == 12:
            category_name = 'Shoes'
        elif row['attribute_set_id'] == 13:
            category_name = 'Bags'
        elif row['attribute_set_id'] == 15:
            category_name = 'Underwear'
        elif row['attribute_set_id'] == 16:
            category_name = 'Accessories'
        elif row['attribute_set_id'] == 11:
            category_name = 'Beauty'
        else:
            category_name = 'Other'
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(row['attribute_set_id'])
        #rb.append(end)
        total.append(rb)
    #db.close()
     
    #总可购商品数量，按大类显示数目
    cursor.execute("select p1.attribute_set_id,count(p1.attribute_set_id) as count from catalog_product_entity p1 left join catalog_product_entity_int p2 on p1.entity_id=p2.entity_id and p2.attribute_id='91' left join catalog_product_entity_int p3 on p3.entity_id=p1.entity_id and p3.attribute_id='84' where p2.value = '4' and p3.value = '1' group by p1.attribute_set_id")
    data = cursor.fetchall()
    total_1 = []
    for row in data:
        rb = []
        #achou=10服装 | icy=12鞋子 16配饰 11美妆 | vava= 13包 15内衣
        rb.append(row['count'])
        rb.append(row['attribute_set_id'])
        rb.append(md5id)
        total_1.append(rb)
    
    #总商品效果值累加，按大类排序
    cursor.execute("select attribute_set_id,sum(effect) as sum from table_auto_order group by attribute_set_id;");
    data = cursor.fetchall()
    total_2 = []
    for row in data:
        rb = []
        rb.append(row['sum'])
        rb.append(row['attribute_set_id'])
        rb.append(md5id)
        total_2.append(rb)

    #喜爱清单数量,按大类显示数量总和
    cursor.execute("select p2.attribute_set_id,count(p1.product_id) as count from wishlist_item p1 left join catalog_product_entity p2 on p2.entity_id = p1.product_id where p1.added_at > '" + begin + "' and p1.added_at < '" + end + "' group by p2.attribute_set_id")
    data = cursor.fetchall()
    total_3 = []
    for row in data:
        rb = []
        rb.append(row['count'])
        rb.append(row['attribute_set_id'])
        rb.append(md5id)
        total_3.append(rb)

    #db1 = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    #按大类先插入数据
    to = [['10','Clothers','achou'],['12','Shoes','icy'],['13','Bags','vava'],['15','Underwear','vava'],['16','Accessories','icy'],['11','Beauty','icy']]
    for row in to:
        row.append(md5id)
        row.append(end)
    cursor1 = db1.cursor()
    sql = "insert into sales_report_info(category_id,category_name,name,md5id,created_at) value(%s,%s,%s,%s,%s)"
    cursor1.executemany(sql,to)
    db1.commit()    

    #插入
    #cursor1 = db1.cursor()
    #sql = "insert into sales_report_info(md5id,name,category_id,category_name,product_num,created_at) value(%s,%s,%s,%s,%s,%s)"
    #cursor1.executemany(sql,total)
    #db1.commit()

    #更新大类上新数量
    #cursor1 = db1.cursor()
    #sql = "update sales_report_info set product_num = %s where md5id = %s and category_id = %s"
    sql = "update sales_report_info set product_num = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()
    #更新大类商品总数
    sql1 = "update sales_report_info set total_product_num = %s where category_id = %s and md5id = %s"
    cursor1.executemany(sql1,total_1)
    db1.commit()
    #更新大类商品总效果值
    sql2 = "update sales_report_info set effect = %s where category_id = %s and md5id = %s"
    cursor1.executemany(sql2,total_2)
    db1.commit()
    #更新大类商品喜爱清单数量
    sql3 = "update sales_report_info set wishlist_num = %s where category_id = %s and md5id = %s"
    cursor1.executemany(sql3,total_3)
    db1.commit()

    #商品浏览数
    cursor.execute("select p2.attribute_set_id,count(p2.attribute_set_id) as count from report_viewed_product_index p1 left join catalog_product_entity p2 on p2.entity_id = p1.product_id where p1.added_at > '" + begin + "' and p1.added_at < '" + end + "' group by p2.attribute_set_id;")
    data = cursor.fetchall()
    total_view = []
    for row in data:
        rb = []
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(row['attribute_set_id'])
        total_view.append(rb)
    sql = "update sales_report_info set product_view = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total_view)
    db1.commit()


    #购物车数量
    cursor.execute("select p3.attribute_set_id,count(p3.attribute_set_id) as count from sales_flat_quote p1 left join sales_flat_quote_item p2 on p2.quote_id=p1.entity_id left join catalog_product_entity p3 on p3.entity_id=p2.product_id where p1.is_active = '1' and p2.parent_item_id is null and p2.created_at > '" + begin + "' and p2.created_at < '" + end + "' group by p3.attribute_set_id;")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        rb.append(row['count'])
        rb.append(md5id)
        rb.append(row['attribute_set_id'])
        print rb
        total.append(rb)
    sql = "update sales_report_info set cart_num = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()

    #喜爱清单数量,按大类显示数量总和
    cursor.execute("select added_at,product_id from wishlist_item where added_at > '" + begin + "' and added_at < '" + end + "'")
    data = cursor.fetchall()
    total = []


    #购物车金额
    cursor.execute("select p3.attribute_set_id,sum(p4.value) as price,sum(p5.value) as special_price from sales_flat_quote p1 left join sales_flat_quote_item p2 on p2.quote_id=p1.entity_id left join catalog_product_entity p3 on p3.entity_id=p2.product_id left join catalog_product_entity_decimal p4 on p4.entity_id = p2.product_id and p4.attribute_id='64' left join catalog_product_entity_decimal p5 on p5.entity_id = p2.product_id and p5.attribute_id='65' where p1.is_active = '1' and p2.parent_item_id is null and p2.created_at > '" + begin + "' and p2.created_at < '" + end + "' group by p3.attribute_set_id;")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        rb.append(row['special_price'])
        rb.append(md5id)
        rb.append(row['attribute_set_id'])
        print rb
        total.append(rb)
    sql = "update sales_report_info set cart_amount = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()


    #订单商品数量，按大类统计
    cursor.execute("select pp.attribute_set_id,count(pp.attribute_set_id) as count from sales_flat_order a1 left join sales_flat_order_item a2 on a2.order_id=a1.entity_id left join catalog_product_entity pp on pp.entity_id = a2.product_id left join catalog_product_entity_varchar a3 on a3.entity_id=a2.product_id and a3.attribute_id='217' left join isoften_exchange_rate a4 on a3.value = a4.currency_type left join catalog_product_entity_varchar a5 on a5.entity_id = a2.product_id and a5.attribute_id='218' left join catalog_product_entity_varchar a6 on a6.entity_id=a2.product_id and a6.attribute_id='219' where a2.parent_item_id is NULL and a1.created_at > '" + begin + "' and a1.created_at < '" + end + "' and a1.status in ('doing','processing','delivered','completed') group by pp.attribute_set_id;")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        rb.append(row['count'])
        rb.append(row['attribute_set_id'])
        rb.append(md5id)
        total.append(rb)
    sql = "update sales_report_info set sales_num = %s where category_id = %s and md5id = %s"
    cursor1.executemany(sql,total)
    db1.commit()



    #订单金额
    cursor.execute("select pp.attribute_set_id,a1.increment_id,a1.status,a1.total_item_count as total_count,a1.grand_total as final_price,a1.subtotal as product_total_price,a1.base_discount_amount as discount,a1.shipping_amount as shipping_cost,a2.product_id,a2.sku,a2.qty_ordered as sku_number,a2.price,a3.value as currency,a4.exchange_rate,a5.value as org_pri,a6.value as now_pri from sales_flat_order a1 left join sales_flat_order_item a2 on a2.order_id=a1.entity_id left join catalog_product_entity pp on pp.entity_id = a2.product_id left join catalog_product_entity_varchar a3 on a3.entity_id=a2.product_id and a3.attribute_id='217' left join isoften_exchange_rate a4 on a3.value = a4.currency_type left join catalog_product_entity_varchar a5 on a5.entity_id = a2.product_id and a5.attribute_id='218' left join catalog_product_entity_varchar a6 on a6.entity_id=a2.product_id and a6.attribute_id='219' where a2.parent_item_id is NULL and a1.created_at > '" + begin + "' and a1.created_at < '" + end + "' and a1.status in ('doing','processing','delivered','completed');")
    data = cursor.fetchall()
    total = []
    for row in data:
        rb = []
        sales = float(row['sku_number'] * row['price'])
        freight_cost = float((row['shipping_cost'] / row['total_count']) * row['sku_number'])
        if row['now_pri']:
            cost = Decimal(row['now_pri']) * Decimal(row['exchange_rate']) * row['sku_number']
        else:
            cost = Decimal(row['org_pri']) * Decimal(row['exchange_rate']) * row['sku_number']
        profit = Decimal(sales) - cost
        rb.append(row['attribute_set_id'])
        rb.append(sales)
        rb.append(freight_cost)
        rb.append(cost)
        rb.append(profit)
        print rb
        total.append(rb)
    totals = []    
    for xb in [10,12,13,15,16,11]:
        total_sales = 0
        total_freight_cost = 0
        total_cost = 0
        total_profit = 0
        rb = []
        for row in total:
            if row[0] == xb:
                total_sales = total_sales + row[1]
                total_freight_cost = total_freight_cost + row[2]
                total_cost = total_cost + row[3]
                total_profit = total_profit + row[4]
        rb.append(total_sales)
        rb.append(total_cost)
        rb.append(total_freight_cost)
        rb.append(total_profit)
        rb.append(xb)
        totals.append(rb)
    print totals
        #print str(xb) + ' ' + str(total_sales) + ' ' + str(total_freight_cost) + ' ' + str(total_cost) + ' ' + str(total_profit)
    sql = "update sales_report_info set sales_amount = %s,sales_cost = %s,freight_cost = %s,profit_amount = %s where category_id = %s"
    cursor1.executemany(sql,totals)
    db1.commit()



    #商品点击贡献百分比，大类点击占总点击百分比
    #cursor1.execute("select category_id,product_view from sales_report_info where md5id='" + md5id + "'")
    #data = cursor.fetchall()
    total_product_view = 0
    for row in total_view:
        total_product_view = total_product_view + row[0]
    total = []
    for row in total_view:
        rb = []
        product_view_percentage = Decimal(row[0]) / Decimal(total_product_view)
        rb.append(product_view_percentage)
        rb.append(md5id)
        rb.append(row[2])
        total.append(rb)
    print total
    sql = "update sales_report_info set view_percentage = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()


   #商品利润百分比，商品按大类利润占总利润百分比
    #cursor1.execute("select category_id,profit_amount from sales_report_info where md5id='" + md5id + "'")
    #data = cursor.fetchall()
    total_profit_amount = 0
    for row in totals:
        total_profit_amount = total_profit_amount + row[3]
    total = []
    for row in totals:
        rb = []
        profit_amount_percentage = Decimal(row[3]) / Decimal(total_profit_amount)
        rb.append(profit_amount_percentage)
        rb.append(md5id)
        rb.append(row[4])
        print rb
        total.append(rb)
    sql = "update sales_report_info set profit_percentage = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()

   #商品利润率，商品大类利润占大类销售百分比
    #cursor1.execute("select category_id,sales_amount,profit_amount from sales_report_info where md5id='" + md5id + "'")
    #data = cursor.fetchall()
    total = []
    for row in totals:
        rb = []
        if row[0] == 0:
            profit_amount_percentage = 0
        else:
            profit_amount_percentage = Decimal(row[3]) / Decimal(row[0])
        rb.append(profit_amount_percentage)
        rb.append(md5id)
        rb.append(row[4])
        print rb
        total.append(rb)
    sql = "update sales_report_info set profit_rate = %s where md5id = %s and category_id = %s"
    cursor1.executemany(sql,total)
    db1.commit()


    db.close()
    db1.close()


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
