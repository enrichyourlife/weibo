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

#配置连接数据库 在这里修改成自己的数据库相关参数
host = 'localhost' #地址
port = 5432 #端口号
dbname = 'wbdb'
user = 'postgres' # 用户名
password = 'root' # 密码
#代理url
proxurl='自己的代理地址'


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
    #         # urla='https://wapi.http.linkudp.com/index/index/save_white?neek=102550&appkey=app的key&white='+str(ip[0])
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
    for item in arydata:
        for i in item['card_group']:
            msgdata = {'createdtim': '', 'pagemsgurl': '', 'mid': '', 'mblogtext': '', 'reposts_count': '',
                       'comments_count': '', 'attitudes_count': '',
                       'userid': '', 'usernick': '', 'userpage': '', 'userfans': '',
                       'userfollows': '','usergender':'', 'poimsg': '', 'poilg': '',
                       'poiwb': '', 'poitb': '', 'poiid': '', 'poiname': '', 'poilat': '', 'poilng': '',
                       'usersunshinecredit':'','userbirthday':'','usercreatedtime':'','userdescription':'','userlocation':'','usereducation':'','useriplocation':'','userdesctext':''
                       }
            try:
                mblog = i['mblog']
            except:
                continue;
            # 发布时间
            try:
                msgdata['createdtim']  = mblog['created_at']  # 时间格式 "created_at": "Sat Aug 07 14:17:36 +0800 2021"
            except:
                pass
            # 详情页url
            try:
                msgdata['pagemsgurl'] = i['scheme']
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
                msgdata['userid'] = userdata['id']
                try:
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
                        'uid': userdata['id'],
                    }

                    userResponse = requests.get('https://weibo.com/ajax/profile/detail', params=params, cookies=cookies, headers=headers)
                    # userurl='https://weibo.com/ajax/profile/detail?uid='+str(msgdata['userid'])
                    # userResponse = requests.get(url=userurl, timeout=15).json()
                    userMsg = userResponse.json()
                    try:
                        msgdata['usersunshinecredit'] =userMsg['data']['sunshine_credit']['level']
                    except:
                        pass
                    try:
                        msgdata['userbirthday'] =userMsg['data']['birthday']
                    except:
                        pass
                    try:
                        msgdata['usercreatedtime'] =userMsg['data']['created_at']
                    except:
                        pass
                    try:
                        msgdata['userdescription'] =userMsg['data']['description']
                    except:
                        pass
                    try:
                        msgdata['userlocation'] =userMsg['data']['location']
                    except:
                        pass
                    try:
                        msgdata['usereducation'] =userMsg['data']['education']['school']
                    except:
                        pass
                    try:
                        msgdata['useriplocation'] =userMsg['data']['ip_location']
                    except:
                        pass
                    try:
                        msgdata['userdesctext'] =userMsg['data']['desc_text']
                    except:
                        pass
                    print(userMsg)
                except Exception as e:
                    print(e)
                    pass
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
                # 用户性别
                msgdata['usergender']  = userdata['gender']
            except:
                # print(userdata)
                pass
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
                poiresponse=''
                while 1:
                    try:
                        poiresponse = requests.get(url=poimsgurl, timeout=80)
                        break
                    except:
                        nn += 1
                    if nn>10:
                        break
                try:
                    pointmsg = poiresponse.json()
                    msgdata['poiname'] = pointmsg['name']
                except:
                    pass
                try:
                    msgdata['poilat'] = pointmsg['lat']
                except:
                    pass
                try:
                    msgdata['poilng'] = pointmsg['lng']
                except:
                    pass
                print('爬取一条数据成功！')
            except:
                pass
            # print(msgdata)
            alldata.append(msgdata)
            # 坐标信息为高德坐标

def sqlconnect(datalist):
    n=1
    for i in datalist:
        try:
            conn = psy.connect(host=host, user=user, password=password, port=port, database=dbname)
            sqlcmd = "insert into usertb (createdtime,pageurl,mid,mblogtext,reposts,comments,attitudes,userid,usernick,userpageurl,userfans,userfollows,usergender,poimsg,poilg,poiwb,poitb,poiid,poiname,lat,lng,usersunshinecredit,userbirthday,usercreatedtime,userdescription,userlocation,usereducation,useriplocation,userdesctext) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur = conn.cursor()
            params = (i['createdtim'],i['pagemsgurl'],i['mid'],i['mblogtext'],i['reposts_count'],i['comments_count'],i['attitudes_count'],i['userid']
                      ,i['usernick'],i['userpage'],i['userfans'],i['userfollows'],i['usergender'],i['poimsg'],i['poilg'],i['poiwb']
                      ,i['poitb'],i['poiid'],i['poiname'],i['poilat'],i['poilng'],
                      i['usersunshinecredit'],i['userbirthday'],i['usercreatedtime'],i['userdescription'],i['userlocation'],i['usereducation'],i['useriplocation'],i['userdesctext'],
                      )
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
        data=response.json()
        getwbmsg(data)
        try:
            return data['data']['pageInfo']['since_id']
        except:
            return ''
    except:
        try:
            return data['data']['pageInfo']['since_id']
        except:
            return ''
        # print('网络连接超时')


if __name__ == '__main__':
    while 1:
        #获取代理
        # getprox()
        #选择获取的类型，最热、最新、全部
        # 位置代码 上海 23103600168008631000000000000 成都 10080814bf5c897776f11648134a65c8365b77_-_lbs
        citycode = '10080814bf5c897776f11648134a65c8365b77'
        typecode = ['_-_sort_time','_-_feed','_-_lbs','_-_recommend','_-_soul']
        # typecode = ['']
        alldata=[]
        urllist=[]
        #获取城市代码 citycode

            # {name: "最新评论", containerid: "10080814bf5c897776f11648134a65c8365b77_-_feed"}
            # {name: "最新发帖", containerid: "10080814bf5c897776f11648134a65c8365b77_-_sort_time"}
            # {name: "热门", containerid: "10080814bf5c897776f11648134a65c8365b77_-_recommend"}
            # {name: "精华", containerid: "10080814bf5c897776f11648134a65c8365b77_-_soul"}
            # wbhoturl= 'https://m.weibo.cn/api/container/getIndex?containerid=10080814bf5c897776f11648134a65c8365b77_-_lbs&lcardid=frompoi&extparam=frompoi&luicode=10000011&lfid=100103type%3D1%26q%3D%E6%88%90%E9%83%BD&since_id='+str(i)
        for p in typecode:
            since_id=''
            for i in range(1, 51):
                alldata=[]
                # wbhoturl = 'https://m.weibo.cn/api/container/getIndex?containerid=' + citycode + p+ '&luicode=10000011&lfid='+citycode+'&count=25&page_type=01&page='+str(i)
                wbhoturl = 'https://m.weibo.cn/api/container/getIndex?containerid=' + citycode + p+ '&luicode=10000011&lfid=100103type%3D1%26q%3D%E6%88%90%E9%83%BD&since_id=' + str(since_id)
                since_id=getrespons(wbhoturl);
                print('pageNum=',i)
                print('since_id=',since_id)
                print('开始数据存储!')
                sqlconnect(alldata)
                # urllist.append(wbhoturl)
        # thredg(urllist,5,getrespons)
        #数据存储进mysql
        print('存储完成!')
        print('休眠十分钟后再次获取!')
        time.sleep(600)
