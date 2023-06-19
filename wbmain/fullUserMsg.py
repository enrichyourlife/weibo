# coding: utf-8
# -- coding: utf-8 --**
import json
import re
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import pymysql
import psycopg2 as psy
import math

#配置连接数据库 在这里修改成自己的数据库相关参数
host = 'localhost' #地址
port = 5432 #端口号
dbname = 'wbdb'
user = 'postgres' # 用户名
password = 'root' # 密码


#多线程封装
def thredg(datalist,thn,functionName):
    p = ThreadPoolExecutor(thn)  # 线程池 #如果不给定值，默认cup*5
    l = []
    for i in datalist:
        obj = p.submit(functionName, i)  # 相当于apply_async异步方法
        l.append(obj)
    p.shutdown()  # 默认有个参数wite=True (相当于close和join)
    [obj.result() for obj in l]


alldata=[]
def getUserDetil(userId):
    global userMsg
    cookies = {
        'XSRF-TOKEN': 'NGwn_A0AD5Le-QomnMZ8Zd5I',
        'SUB': '_2AkMT0d90f8NxqwFRmfkWy2vhaIV2ywHEieKljS6vJRMxHRl-yT9vqlRatRB6OFHxm48J734ar3cmDnfdV72f4v3cIGnI',
        'SUBP': '0033WrSXqPxfM72-Ws9jqgMF55529P9D9WFFlrP74qVh8n-FXRO.jJaD',
        'WBPSESS': 'HOKMwFaOhMG7Cl30d6Y-8U3ADf7JKUVle9dpRFLRvUaERJV9m8kjMbV9k2HbUU8HySgEvhkWBgnGP6do7b1vSay9L1i7GVRE-OhAFTZpldgljdqxNVP-98uWyiCAlzqK',
    }
    headers = {
        'authority': 'weibo.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Microsoft Edge";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.51',
    }
    params = {
        'uid': str(userId),
    }
    userResponse = requests.get('https://weibo.com/ajax/profile/detail', params=params, cookies=cookies, headers=headers)
    userMsg = userResponse.json()
    return userMsg

def getuserhsty(userMsg):
    global alldata
    msgdata = {'usergender':'', 'usersunshinecredit':'','userbirthday':'','usercreatedtime':'','userdescription':'','userlocation':'','usereducation':'','useriplocation':'','userdesctext':''
               }
    # 用户id
    try:
        try:
            msgdata['usergender'] = userMsg['data']['gender']
        except:
            pass
        try:
            msgdata['usersunshinecredit'] = userMsg['data']['sunshine_credit']['level']
        except:
            pass
        try:
            msgdata['userbirthday'] = userMsg['data']['birthday']
        except:
            pass
        try:
            msgdata['usercreatedtime'] = userMsg['data']['created_at']
        except:
            pass
        try:
            msgdata['userdescription'] = userMsg['data']['description']
        except:
            pass
        try:
            msgdata['userlocation'] = userMsg['data']['location']
        except:
            pass
        try:
            msgdata['usereducation'] = userMsg['data']['education']['school']
        except:
            pass
        try:
            msgdata['useriplocation'] = userMsg['data']['ip_location']
        except:
            pass
        try:
            msgdata['userdesctext'] = userMsg['data']['desc_text']
        except:
            pass
        alldata=msgdata
    except Exception as e:
        print(e)
        pass


# def getcounts(jsondata):
#     counts = jsondata['data']['cardlistInfo']['total']  # 用户总微博数
#     return counts

#获取数据库中去重的用户id
def getalluser():
    conn = psy.connect(database=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    # cur.execute("select userid from shdata where mid in (select max(mid) from shdata group by userid)")
    cur.execute("select userid from shdata where  usersunshinecredit is NULL AND  mid in (select max(mid)  from shdata group by userid)")
    data_records = cur.fetchall()
    cycl = []
    for i in data_records:
        cycl.append(i[0])
    return set(cycl)  # 返回全部周期查询结果



#更新数据
def sqlconnect(i,userid):
    try:
        print(i,11111111)
        conn = psy.connect(host="localhost", user="postgres", password="root", port=5432, database="wbdb")
        sqlcmd="UPDATE shdata SET usergender='"+i['usergender']+"',usersunshinecredit='"+i['usersunshinecredit']+"',userbirthday='"+i['userbirthday']+"',usercreatedtime='"+i['usercreatedtime']+"',userdescription='"+i['userdescription']+"',userlocation='"+i['userlocation']+"',usereducation='"+i['usereducation']+"',useriplocation='"+i['useriplocation']+"',userdesctext='"+i['userdesctext']+"' WHERE userid = '"+userid+"'"
        # sqlcmd = "insert into usertb (createdtime,pageurl,mid,mblogtext,reposts,comments,attitudes,userid,usernick,userpageurl,userfans,userfollows,usergender,poimsg,poilg,poiwb,poitb,poiid,poiname,lat,lng,usersunshinecredit,userbirthday,usercreatedtime,userdescription,userlocation,usereducation,useriplocation,userdesctext) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur = conn.cursor()
        cur.execute(sqlcmd)  # 执行sql语句
        conn.commit()
        cur.close()
        print('成功更新一位用户信息')
    except psy.Error as e:
        print('数据',i)
        print(e)
        # print(params)
        cur.close()
        pass

if __name__ == '__main__':

    #获取全部用户id
    allidlist=getalluser()
    print('共'+str(len(allidlist))+'位用户数据准备补全')
    #依次获取每个用户的全部微博数据
    for i in allidlist:
        alldata = {}
        try:
            usermsg=getUserDetil(i)
            getuserhsty(usermsg)
            #存储该用户签到微博数据
            if len(alldata)>0:
                try:
                    alldata['userdescription']= alldata['userdescription'].replace("'",",")
                    alldata['usereducation']= alldata['usereducation'].replace("'",",")
                except:
                    pass
                sqlconnect(alldata,i)
        except:
            pass