#!/usr/bin/python
# -*- coding:UTF-8 -*-
################################
##  此脚本将读取文本中的数据，
##  清洗处理后写入文本和导入数据库
##  实例60M的数据几秒处理完
################################

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


pwd = os.getcwd()
fuck_file = pwd + '/' + 'demo.txt'
fuck_file_2 = pwd + '/' + 'demo_result.txt'
fuck_file_3 = pwd + '/' + 'demo_other.txt'
fuck_file_4 = pwd + '/' + 'cdemo_total.txt'
print fuck_file
if os.path.exists(fuck_file):
    db1 = MySQLdb.connect("database_url","database_user","database_passwd","database_name",port=3306,charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
    cursor1 = db1.cursor()
    file = codecs.open(fuck_file, 'r', encoding='utf-8')
    reader = file.read()
    #res = json.loads(res)
    all_line = reader.splitlines()
    t_list = {}#分用户归类，去除重复数据，记录每次打开APP时间和推出APP时间、以及每次使用时长
    c_list = []
    for i in all_line:
        if i.find('id') >= 0:
            continue
        if i.find('|') >= 0:
            customer_id = i.split('\t')[1]
            if customer_id not in c_list:
                c_list.append(customer_id)
            data = i.replace('4516;|','|').replace('| ','|').replace('下午','').replace('上午','').split('\t')[3]
            if data.find('|;') < 0:#过滤次数据
                continue
            for ij in data.replace('4516;|','|').split(';'):
                #过滤无效数据
                if ij.find('|2017') >= 0:
                    #获取登录时间及在线时长
                    r_list = []
                    begin_time = ij.replace(' +0000','').replace(' PM','').replace(' AM','').split('|')[3]
                    end_time = data.replace(' +0000','').replace(' AM','').replace(' PM','').split(';')[-2].split('|')[-2]
                    begin_s = time.strptime(begin_time,'%Y-%m-%d %H:%M:%S')
                    begin_time_s = int(time.mktime(begin_s))
                    end_s = time.strptime(end_time,'%Y-%m-%d %H:%M:%S')
                    end_time_s = int(time.mktime(end_s))
                    total_time = end_time_s - begin_time_s
                    #print customer_id +'|' + str(begin_time) + '|' + str(end_time) + '|' + str(total_time)
                    #r_list.append(customer_id)
                    r_list.append(begin_time)
                    r_list.append(end_time)
                    r_list.append(total_time)
                    if not t_list.has_key(customer_id):
                        t_list[customer_id] = []
                    if r_list not in t_list[customer_id]:#去除重复数据
                        t_list[customer_id].append(r_list)
                    break#只需要获取第一条数据中的登陆及登出时间即可
            
    print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
   
    print "****************************"
    #t_list 用户存放：分用户归类，去除重复数据，记录每次打开APP时间和推出APP时间、以及每次使用时长
    line = json.dumps(t_list) + "\n"
    file = codecs.open(fuck_file_2, 'w', encoding='utf-8')
    file.write(line)
    file.close()

    total_result = {}
    e_list = {}#用于存放所有点击的位置数据和起始时间数据
    for i in c_list:
        total_result[i] = []
        e_list[i] = []

    print "==========================="

    for i in t_list:
        rb = {}
        if not t_list[i]: 
            continue
        customer_id = i
        f_login_time = t_list[i][0][0]
        f_logout_time = t_list[i][0][1]
        f_online_time = t_list[i][0][2]
        l_login_time = t_list[i][-1][0]
        l_logout_time = t_list[i][-1][1]
        l_online_time = t_list[i][-1][2]
        
        rb['customer_id'] = i
        rb['f_login_time'] = f_login_time
        rb['f_logout_time'] = f_logout_time
        rb['f_online_time'] = f_online_time
        rb['l_login_time'] = l_login_time
        rb['l_logout_time'] = l_logout_time
        rb['l_online_time'] = l_online_time
        rb['t_login_count'] = len(t_list[i])

        list_total_online = []
        for j in t_list[i]:
            list_total_online.append(j[2])

        rb['t_online_time'] = sum(list_total_online) 
        total_result[i].append(rb)
        #break
        #print rb
    print "++++++++++++++++++++++++++++++++"
    #计算最后7天总时长
    delta = timedelta(days=7)
    for i in t_list:
        if not t_list[i]:
            continue
        c = t_list[i][::-1]
        #最后一次登陆时间为j[1]，根据最后一次登陆时间推出最后七天起始时间cc 
        r = datetime.strptime(c[0][0],'%Y-%m-%d %H:%M:%S')
        #获取最后7天第一次登陆时间cc
        cc = r - delta
        #初始化number=0
        number = 0
        #当j[1]大于cc时，循环累计最后7天登陆时长
        i_count = 0
        for j in c:
            dd = datetime.strptime(j[0],'%Y-%m-%d %H:%M:%S') 
            if dd > cc:
                number = number + j[2]
                i_count = i_count + 1
            else:
                break
        #print '7天总时长:' + str(number)
        total_result[i][0]['7_days_count'] = i_count
        total_result[i][0]['7_days_time'] = number
        total_result[i][0]['7_days_average'] = number / i_count
   
    print total_result

    print "#################################################以上记录登陆信息及时长################################"

    print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
    #记录用户每次浏览具体位置及起始时间
    for i in all_line:
        if i.find('id') >= 0:
            continue
        if i.find('|') >= 0:
            customer_id = i.split('\t')[1]
            data = i.split('\t')[3]
            for ij in data.split(';'):
                #过滤无效数据
                if ij.find('|2017') >= 0:
                    #过滤重复数据，获取浏览位置
                    if ij not in e_list[customer_id]:
                        e_list[customer_id].append(ij)
            #break
        #break
    #print e_list
    line = json.dumps(e_list) + "\n"
    file = codecs.open(fuck_file_3, 'w', encoding='utf-8')
    file.write(line)
    file.close()

    total = []
    for i in e_list:
        #print e_list[i]
        if not total_result[i]:#过滤掉空的数据
            continue
        A = 0
        B = 0
        C = 0
        D = 0
        DD = 0
        E = 0
        F = 0
        G = 0
        H = 0
        I = 0
        J = 0
        JJ = 0
        K = 0
        O = 0
        P = 0
        Q = 0
        T = 0
        R = 0
        S = 0
        begin_time_s = 0
        end_time_s = 0

        for j in e_list[i]:
            data = j.replace('下午','').replace('上午','').split('|')
            #print j
            if data[1] and data[3] and data[4]:
                begin_s = time.strptime(data[3],'%Y-%m-%d %H:%M:%S')
                begin_time_s = int(time.mktime(begin_s))
                end_s = time.strptime(data[4],'%Y-%m-%d %H:%M:%S')
                end_time_s = int(time.mktime(end_s))

            online_time = end_time_s - begin_time_s
            if data[1] == 'A':
                A = A + online_time
            if data[1] == 'B':
                B = B + online_time
            if data[1] == 'C':
                C = C + online_time
            if data[1] == 'D':
                D = D + online_time
            if data[1] == 'DD':
                DD = DD + online_time
            if data[1] == 'E':
                E = E + online_time
            if data[1] == 'F':
                F = F + online_time
            if data[1] == 'G':
                G = G + online_time
            if data[1] == 'H':
                H = H + online_time
            if data[1] == 'I':
                I = I + online_time
            if data[1] == 'JJ':
                JJ = JJ + online_time
            if data[1] == 'K':
                K = K + online_time
            if data[1] == 'O':
                O = O + online_time
            if data[1] == 'P':
                P = P + online_time
            if data[1] == 'Q':
                Q = Q + online_time
            if data[1] == 'R':
                R = R + online_time
            if data[1] == 'S':
                S = S + online_time
            if data[1] == 'T':
                T = T + online_time
        total_result[i][0]['A'] = A
        total_result[i][0]['B'] = B
        total_result[i][0]['C'] = C
        total_result[i][0]['D'] = D
        total_result[i][0]['DD'] = DD
        total_result[i][0]['E'] = E
        total_result[i][0]['F'] = F
        total_result[i][0]['G'] = G
        total_result[i][0]['H'] = H
        total_result[i][0]['I'] = I
        total_result[i][0]['J'] = J
        total_result[i][0]['JJ'] = JJ
        total_result[i][0]['K'] = K
        total_result[i][0]['O'] = O
        total_result[i][0]['P'] = P
        total_result[i][0]['Q'] = Q
        total_result[i][0]['R'] = R
        total_result[i][0]['S'] = S
        total_result[i][0]['T'] = T
        #break


    line = json.dumps(total_result) + "\n"
    file = codecs.open(fuck_file_4, 'w', encoding='utf-8')
    file.write(line)
    file.close()
    

    total = []
    for i in total_result:
        if not total_result[i]:
            continue
        rb = []
        rb.append(i)
        rb.append(total_result[i][0]['f_login_time'])
        rb.append(total_result[i][0]['f_logout_time'])
        rb.append(total_result[i][0]['f_online_time'])
        rb.append(total_result[i][0]['l_login_time'])
        rb.append(total_result[i][0]['l_logout_time'])
        rb.append(total_result[i][0]['l_online_time'])
        rb.append(total_result[i][0]['7_days_count'])
        rb.append(total_result[i][0]['7_days_time'])
        rb.append(total_result[i][0]['7_days_average'])
        rb.append(total_result[i][0]['t_login_count'])
        rb.append(total_result[i][0]['t_online_time'])
        rb.append(total_result[i][0]['A'])
        rb.append(total_result[i][0]['B'])
        rb.append(total_result[i][0]['C'])
        rb.append(total_result[i][0]['D'])
        rb.append(total_result[i][0]['DD'])
        rb.append(total_result[i][0]['E'])
        rb.append(total_result[i][0]['F'])
        rb.append(total_result[i][0]['G'])
        rb.append(total_result[i][0]['H'])
        rb.append(total_result[i][0]['I'])
        rb.append(total_result[i][0]['J'])
        rb.append(total_result[i][0]['JJ'])
        rb.append(total_result[i][0]['K'])
        rb.append(total_result[i][0]['O'])
        rb.append(total_result[i][0]['P'])
        rb.append(total_result[i][0]['Q'])
        rb.append(total_result[i][0]['R'])
        rb.append(total_result[i][0]['S'])
        rb.append(total_result[i][0]['T'])
        total.append(rb)
    sql = "insert into customer_behavior(customer_id,f_login_time,f_logout_time,f_online_time,l_login_time,l_logout_time,l_online_time,7_days_count,7_days_time,7_days_average,t_login_count,t_online_time,A,B,C,D,DD,E,F,G,H,I,J,JJ,K,O,P,Q,R,S,T) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor1.executemany(sql,total)
    db1.commit()



print total_result
os._exit(0)