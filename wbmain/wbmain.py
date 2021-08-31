# coding: utf-8
import re
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import pymysql
import psycopg2 as psy
'''
    全局变量
'''

#代理url
proxurl='http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='

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
    # #添加ip白名单
    # while 1:
    #     try:
    #         headers = {
    #             'Connection': 'keep-alive',
    #             'sec-ch-ua': '^\\^',
    #             'sec-ch-ua-mobile': '?0',
    #             'Upgrade-Insecure-Requests': '1',
    #             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
    #             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    #             'Sec-Fetch-Site': 'none',
    #             'Sec-Fetch-Mode': 'navigate',
    #             'Sec-Fetch-User': '?1',
    #             'Sec-Fetch-Dest': 'document',
    #             'Accept-Language': 'zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6',
    #         }
    #         # urla='https://wapi.http.linkudp.com/index/index/save_white?neek=102550&appkey=5b1f1a5b143dd4b242956c87080df4b8&white='+str(ip[0])
    #         respon = requests.get(url=urla,headers=headers)
    #         msg=respon.json()
    #         if msg['code']==115:
    #             print('网络优化完成!欢迎您的使用!')
    #             break
    #         time.sleep(1)
    #     except:
    #         pass
    # try:
    #     getpox = requests.get(url=proxurl)
    #     data = getpox.json()
    #     allprox = prox(data)
    # except:
    pass

#全局数据存储
alldata=[]
def getwbmsg(jsondata):

    arydata = jsondata['data']['cards']
    for i in arydata:
        msgdata = {'createdtim': '', 'pagemsgurl': '', 'mid': '', 'mblogtext': '', 'reposts_count': '',
                   'comments_count': '', 'attitudes_count': '',
                   'userid': '', 'usernick': '', 'userpage': '',  'userfans': '',
                   'userfollows': '', 'poimsg': '', 'poilg': '',
                   'poiwb': '', 'poitb': '', 'poiid': '', 'poiname': '', 'poilat': '', 'poilng': ''}
        mblog = i['card_group'][0]['mblog']
        # 发布时间
        try:
            msgdata['createdtim']  = mblog['created_at']  # 时间格式 "created_at": "Sat Aug 07 14:17:36 +0800 2021"
        except:
            pass
        # 详情页url
        try:
            msgdata['pagemsgurl'] = i['card_group'][0]['scheme']
        except:
            pass
        # 微博内容唯一标识id  与详情页中的id匹配
        try:
            msgdata['mid']  = mblog['mid']
            # 微博内容
            mbtext = mblog['text']
            pattern = re.compile(r'<[^>]+>', re.S)
            msgdata['mblogtext']  = pattern.sub('', mbtext) #去除html标签部分
        except:
            break
        # 转发数
        try:
            reposts_count = mblog['reposts_count']
        except:
            reposts_count=0
        msgdata['reposts_count']=reposts_count
        # 评论数
        try:
            comments_count = mblog['comments_count']
        except:
            comments_count =0
        msgdata['comments_count']=comments_count
        # 点赞数
        try:
            attitudes_count = mblog['attitudes_count']
        except:
            attitudes_count=0
        msgdata['attitudes_count'] = attitudes_count
        ####用户信息
        userdata = mblog['user']
        # 用户id
        try:
            msgdata['userid']  = userdata['id']
            # 用户名称
            msgdata['usernick'] = userdata['screen_name']
            # 用户主页url
            msgdata['userpage'] = userdata['profile_url']
            # 用户个性签名/描述
            # msgdata['userdescription']  = userdata['description']
            # 用户粉丝数量
            msgdata['userfans']  = userdata['followers_count']
            # 用户关注数量
            msgdata['userfollows']  = userdata['follow_count']
        except:
            print(userdata)
        ###打卡地点信息
        try:
            poi = mblog['page_info']
            # poi名称标题
            # poiname=poi['page_title']
            # poi描述或地址
            msgdata['poimsg']  = poi['content1']
            # poi描述 48人来过 59条微博 58张图片
            poimsg2 = poi['content2']
        except:
            pass
        try:
            msgdata['poilg']  = re.findall('(\d+)人来过', poimsg2)[0]
            msgdata['poiwb']  = re.findall('(\d+)条微博', poimsg2)[0]
            msgdata['poitb']  = re.findall('(\d+)张图片', poimsg2)[0]
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
            poiid= re.findall(pt, poiurl)[0]  # 提取出 B2094257D06DA7FF469E
            msgdata['poiid']=poiid
            # "page_url": "https://m.weibo.cn/p/index?containerid=2306570042B2094257D06DA7FF469E&luicode=10000011&lfid=23103600168008631000000000000_new"
            # 拼接成查询地点的url  https://place.weibo.com/wandermap/pois?poiid=B2094257D06DA7FF469E
            poimsgurl = 'https://place.weibo.com/wandermap/pois?poiid=' + poiid
            nn=1
            while 1:
                try:
                    response = requests.get(url=poimsgurl, timeout=80)
                    break
                except:
                    nn += 1
                if nn>10:
                    break
            pointmsg = response.json()
            msgdata['poiname'] = pointmsg['name']
            msgdata['poilat'] = pointmsg['lat']
            msgdata['poilng']  = pointmsg['lng']
            print('爬取一条数据成功！')
        except:
            break
        print(msgdata)
        alldata.append(msgdata)
        # 坐标信息为高德坐标

def sqlconnect(datalist):
    n=1
    for i in datalist:
        try:
            conn = psy.connect(host="localhost", user="postgres", password="root", port=5432, database="wbdb")
            sqlcmd = "insert into poitb (createdtime,pageurl,mid,mblogtext,reposts,comments,attitudes,userid,usernick,userpageurl,userfans,userfollows,poimsg,poilg,poiwb,poitb,poiid,poiname,lat,lng) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur = conn.cursor()
            params = (i['createdtim'],i['pagemsgurl'],i['mid'],i['mblogtext'],i['reposts_count'],i['comments_count'],i['attitudes_count'],i['userid']
                      ,i['usernick'],i['userpage'],i['userfans'],i['userfollows'],i['poimsg'],i['poilg'],i['poiwb']
                      ,i['poitb'],i['poiid'],i['poiname'],i['poilat'],i['poilng'])
            cur.execute(sqlcmd, params)  # 执行sql语句
            conn.commit()
            cur.close()
            print('成功存储一条信息')
            n+=1
        except psy.Error as e:
            # print(e)
            # print(params)
            cur.close()
            pass
    print('成功存储'+str(n)+'条数据')

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
        response = requests.get(url=fullurl, timeout=80)
        getwbmsg(response.json())
    except:
        print('网络连接超时')


if __name__ == '__main__':
    while 1:
        #获取代理
        # getprox()
        #选择获取的类型，最热、最新、全部
        # 位置代码 上海 23103600168008631000000000000
        citycode = '23103600168008631000000000000'
        typecode = ['', '_hot', '_new']
        # typecode = ['']
        alldata=[]
        urllist=[]
        #获取城市代码 citycode
        for i in range(1,51):
            for p in typecode:
                wbhoturl = 'https://m.weibo.cn/api/container/getIndex?containerid=' + citycode + p+ '&luicode=10000011&lfid=23065700428008631000000000000&count=25&page_type=01&page='+str(i)
                urllist.append(wbhoturl)
        thredg(urllist,30,getrespons)
        #数据存储进mysql
        print('开始数据存储!')
        sqlconnect(alldata)
        print('存储完成!')
        print('休眠十分钟后再次获取!')
        time.sleep(600)
        ##