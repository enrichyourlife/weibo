# coding: utf-8
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

proxurl ='http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='#line:61

#代理服务器
def prox(data):
    proxyHost = data['data'][0]['ip']
    proxyPort = data['data'][0]['port']
    proxyMeta = "http://%(host)s:%(port)s" % {
        "host" : proxyHost,
        "port" : proxyPort,
    }
    proxies = {
        "http"  : proxyMeta,
        "https"  : proxyMeta
    }
    return proxies

allprox=''
#ip获取
def getprox():
    try:
        r = requests.get("http://txt.go.sohu.com/ip/soip")
        ip = re.findall(r'\d+.\d+.\d+.\d+', r.text)
        print('正在更换ip!')
    except:
        print('请检查你的网络!')
    #添加ip白名单
    while 1:
        try:
            headers = {
                'Connection': 'keep-alive',
                'sec-ch-ua': '^\\^',
                'sec-ch-ua-mobile': '?0',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-User': '?1',
                'Sec-Fetch-Dest': 'document',
                'Accept-Language': 'zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6',
            }
            urla='https://wapi.http.linkudp.com/index/index/save_white?neek=102550&appkey=5b1f1a5b143dd4b242956c87080df4b8&white='+str(ip[0])
            respon = requests.get(url=urla,headers=headers)
            msg=respon.json()
            if msg['code']==115:
                # print('网络优化完成!欢迎您的使用!')
                break
            time.sleep(1)
        except:
            pass
    try:
        getpox = requests.get(url=proxurl)
        data = getpox.json()
        allprox = prox(data)
    except:
        pass

#多线程封装
def thredg(datalist,thn,functionName):
    p = ThreadPoolExecutor(thn)  # 线程池 #如果不给定值，默认cup*5
    l = []
    for i in datalist:
        obj = p.submit(functionName, i)  # 相当于apply_async异步方法
        l.append(obj)
    p.shutdown()  # 默认有个参数wite=True (相当于close和join)
    [obj.result() for obj in l]

def getrespons(fullurl):
    # wbhoturl=fullurl
    # response = requests.get(url=fullurl, timeout=80, proxies=allprox)

    try:
        response = requests.get(url=fullurl, timeout=15, proxies=allprox)
        # response = requests.get(url=fullurl, timeout=80)
        # getuserhsty(response.json())
        return response
    except:
        print('网络连接超时')

alldata=[]
def getuserhsty(fullurl):
    jsondata=getrespons(fullurl).json()
    # counts = jsondata['data']['cardlistInfo']['total']  # 用户总微博数
    cards = jsondata['data']['cards']
    if len(cards)==0:
        print('该页无用户数据！')
    else:
        for i in cards:
            msgdata = {'createdtim': '', 'pagemsgurl': '', 'mid': '', 'mblogtext': '', 'reposts_count': '',
                       'comments_count': '', 'attitudes_count': '',
                       'userid': '', 'usernick': '', 'userpage': '', 'userfans': '',
                       'userfollows': '', 'poimsg': '', 'poilg': '',
                       'poiwb': '', 'poitb': '', 'poiid': '', 'poiname': '', 'poilat': '', 'poilng': ''}
            mblog = i['mblog']
            # 发布时间
            try:
                msgdata['createdtim'] = mblog['created_at']  # 时间格式 "created_at": "Sat Aug 07 14:17:36 +0800 2021"
            except:
                pass
            # 详情页url
            try:
                msgdata['pagemsgurl'] = i['card_group'][0]['scheme']
            except:
                pass
            # 微博内容唯一标识id  与详情页中的id匹配
            try:
                msgdata['mid'] = mblog['mid']
                # 微博内容
                mbtext = mblog['text']
                pattern = re.compile(r'<[^>]+>', re.S)
                msgdata['mblogtext'] = pattern.sub('', mbtext)  # 去除html标签部分
            except:
                break
            # 转发数
            try:
                reposts_count = mblog['reposts_count']
            except:
                reposts_count = 0
            msgdata['reposts_count'] = reposts_count
            # 评论数
            try:
                comments_count = mblog['comments_count']
            except:
                comments_count = 0
            msgdata['comments_count'] = comments_count
            # 点赞数
            try:
                attitudes_count = mblog['attitudes_count']
            except:
                attitudes_count = 0
            msgdata['attitudes_count'] = attitudes_count
            ####用户信息
            userdata = mblog['user']
            # 用户id
            try:
                msgdata['userid'] = userdata['id']
                # 用户名称
                msgdata['usernick'] = userdata['screen_name']
                # 用户主页url
                msgdata['userpage'] = userdata['profile_url']
                # 用户个性签名/描述
                # msgdata['userdescription']  = userdata['description']
                # 用户粉丝数量
                msgdata['userfans'] = userdata['followers_count']
                # 用户关注数量
                msgdata['userfollows'] = userdata['follow_count']
            except:
                print(userdata)
            ###打卡地点信息
            try:
                poi = mblog['page_info']
                # poi名称标题
                # poiname=poi['page_title']
                # poi描述或地址
                msgdata['poimsg'] = poi['content1']
                # poi描述 48人来过 59条微博 58张图片
                poimsg2 = poi['content2']
            except:
                pass
            #判断是否为签到打卡地点，仅保留地点类数据
            if poi['type']=='place':
                try:
                    msgdata['poilg'] = re.findall('(\d+)人来过', poimsg2)[0]
                    msgdata['poiwb'] = re.findall('(\d+)条微博', poimsg2)[0]
                    msgdata['poitb'] = re.findall('(\d+)张图片', poimsg2)[0]
                except:
                    pass
                # 地点详情页面url
                try:
                    poiurl = poi['page_url']
                except:
                    pass
                # 可以从详情页提取出高德的poi唯一标识编码BXXXXXXXXXXXXXXXXXXX 再拼接接口获取对应的经纬度信息
                try:
                    pt = '(B.*)&lu'
                    poiid = re.findall(pt, poiurl)[0]  # 提取出 B2094257D06DA7FF469E
                    msgdata['poiid'] = poiid
                    # "page_url": "https://m.weibo.cn/p/index?containerid=2306570042B2094257D06DA7FF469E&luicode=10000011&lfid=23103600168008631000000000000_new"
                    # 拼接成查询地点的url  https://place.weibo.com/wandermap/pois?poiid=B2094257D06DA7FF469E
                    poimsgurl = 'https://place.weibo.com/wandermap/pois?poiid=' + poiid
                    nn = 1
                    while 1:
                        try:
                            response = requests.get(url=poimsgurl, timeout=15, proxies=allprox)
                            break
                        except:
                            nn += 1
                        if nn > 5:
                            break
                    pointmsg = response.json()
                    msgdata['poiname'] = pointmsg['name']
                    msgdata['poilat'] = pointmsg['lat']
                    msgdata['poilng'] = pointmsg['lng']
                    print('爬取一条数据成功！')
                except:
                    break
                print(msgdata)
                alldata.append(msgdata)
                # 坐标信息为高德坐标

def getcounts(jsondata):
    counts = jsondata['data']['cardlistInfo']['total']  # 用户总微博数
    return counts

#获取数据库中去重的用户id
def getalluser():
    conn = psy.connect(database=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute("select userid from poitb where mid in (select max(mid) from poitb group by userid)")
    data_records = cur.fetchall()
    cycl = []
    for i in data_records:
        cycl.append(i[0])
    return set(cycl)  # 返回全部周期查询结果

#获取数据库中去重的用户id
def getalluserd():
    conn = psy.connect(database=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute("select userid from usertb where mid in (select max(mid) from poitb group by userid)")
    data_records = cur.fetchall()
    cycl = []
    for i in data_records:
        cycl.append(i[0])
    return set(cycl)  # 返回全部周期查询结果


#存储数据
def sqlconnect(datalist):
    n=0
    for i in datalist:
        try:
            conn = psy.connect(host="localhost", user="postgres", password="root", port=5432, database="wbdb")
            sqlcmd = "insert into usertb (createdtime,pageurl,mid,mblogtext,reposts,comments,attitudes,userid,usernick,userpageurl,userfans,userfollows,poimsg,poilg,poiwb,poitb,poiid,poiname,lat,lng) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur = conn.cursor()
            params = (i['createdtim'],i['pagemsgurl'],i['mid'],i['mblogtext'],i['reposts_count'],i['comments_count'],i['attitudes_count'],i['userid']
                      ,i['usernick'],i['userpage'],i['userfans'],i['userfollows'],i['poimsg'],i['poilg'],i['poiwb']
                      ,i['poitb'],i['poiid'],i['poiname'],i['poilat'],i['poilng'])
            cur.execute(sqlcmd, params)  # 执行sql语句
            conn.commit()
            cur.close()
            n+=1
            print('成功存储一条信息')
        except psy.Error as e:
            # print(e)
            # print(params)
            cur.close()
            pass
    print('成功存储'+str(n)+'条数据')

if __name__ == '__main__':
    getprox()
    print(allprox)
    #获取全部用户id
    allidlist=getalluser()
    idhas=getalluserd()
    ret = list(set(allidlist) ^ set(idhas))
    print('共'+str(len(ret))+'位用户准备获取')
    #依次获取每个用户的全部微博数据
    for i in allidlist:
        alldata = []
        #每次更换用户就重新清空list
        try:
            userurl='https://m.weibo.cn/api/container/getIndex?containerid=107603'+str(i)+'&count=25&page='
            counts=getcounts(getrespons(userurl).json())
            try:
                pages=math.ceil(counts/25) #计算总页数
            except:
                pages=2
            if pages>50:
                pages=50
            allurl=[]
            print('该用户共'+str(pages)+'页微博')
            for p in range(1,pages):
                pageurl=userurl+str(p)
                allurl.append(pageurl)
            #多线程获取该用户的历史微博 页
            thredg(allurl,15,getuserhsty)
            #存储该用户签到微博数据
            print('共获取到'+str(len(alldata))+'条打卡数据')
            sqlconnect(alldata)
        except:
            sqlconnect(alldata)
            time.sleep(10)
            getprox()