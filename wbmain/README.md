## 微博打卡程序使用说明

程序分为两个部分，**wbmain.py**用于获取最新你的上海市签到打开数据，主要获取用户id
**getUserHsty.py**主要用于从数据库中获取用户id进行用户历史微博数据遍历爬取的操作，并且将用户历史微博数据中包含签到打卡信息的数据进行存储在数据库中



两个程序同时开启即可，并且在文件中配置自己数据库的相关参数

![image-20210913090720261](image-20210913090720261.png)

![image-20210913090756105](image-20210913090756105.png)