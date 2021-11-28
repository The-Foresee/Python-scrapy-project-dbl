import traceback
import scrapy
import time
import pymysql
from pymysql import connections
from pymysql import connect
import uuid as uuid
import xlwt
import types
# import requests
import requests
import pandas as pd
from pymysql.constants import COMMAND
from scrapy import Request
from scrapy.http import request
from scrapy.spiders import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from requests.packages import urllib3

class GuochanqingseSpider(scrapy.Spider):
    name = 'guochanqingse'
    count = 1
    count1 = 0
    values = ()
    # 建立数据库连接
    db = pymysql.connect(host='127.0.0.1', user='root', password='123456', db='dabolu1')
    # 获取游标对象
    cursor = db.cursor()
    #确认数据表是否存在,如果不存在创建数据库表,如果存在提示已存在
    try:
        # 创建数据库，如果数据库已经存在，注意主键不要重复，否则出错
        cursor.execute('create table '
                        'dabolu1_list( '
                           'NO VARCHAR(40) primary key, '   # 主键
                           'name VARCHAR(500), '            # 名称
                           'catalogue VARCHAR(100), '       # 分类
                           'popularity VARCHAR(100), '      # 人气
                           'date VARCHAR(100), '            # 时间
                           'image VARCHAR(100), '           # 图片
                           'video VARCHAR(100) '            # 视频
                        ')')
    except:
        print('数据库已存在！')
    # 插入数据SQL语句
    query1 = """insert into dabolu1_list (NO, name, catalogue, popularity,date,image,video) values (%s,%s,%s,%s,%s,%s,%s)"""
    #模拟请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',#模拟用户浏览器
        'Cookie': 'Cookie:UM_distinctid=17c025388b5202-00a09d0bdf3ff9-b7a103e-100200-17c025388b62e9;CNZZDATA1279979848=1590887380-1632124990-null%7C1632124990',#模拟Cookie
        'Host': '172.86.93.240:6534',#模拟Host地址
        # 'Referer': 'http://172.86.93.240:6534/index.php/vodtype/0CCCCS-1'
    }

    # def __del__(self):
    #     self.db.close()

    # 设置数据库连接时间方法,默认状态八个小时
    def to_connect(self):
        return pymysql.connections.Connection(connect_timeout=1000000)

    # 执行连接数据库的方法
    def is_connected(self):
        """Check if the server is alive"""
        try:
            self.db.ping(reconnect=True)#运用pymysql中的ping方法连接数据库
            print("db is connecting")#输出数据库的连接状态
        except:
            traceback.print_exc()
            self.to_connect()
            print("db reconnect")

    #开始时候执行的方法
    def start_requests(self):
        url = 'http://172.86.93.240:6534'# 爬取网站的地址
        yield Request(url, headers=self.headers, dont_filter=True)# 加载进入要爬取的网站

    #加载视频页面,并获取视频的真正地址(失效)
    def video(self, response):
        #获取视频真正地址(失效状态)
        video = response.xpath('//*[@id="bofang_box"]/iframe/@src').extract_first()
        video = str(video).split('=')
        print(video[1])#打印视频地址

    # 加载商品页每一页中的商品数据信息,并获取商品数据信息,逐一写入数据库
    def list(self, response):
        time.sleep(1)  # 这里的是一秒 爬取过快页面刷新不出来,
        for c in range(1, 15):  # int(total[1]) 循环迭代出每一页包含的商品数据数目,默认是14个商品
            #运用Xpath工具获取网站页面中商品的信息
            # 商品名称
            name = response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/h2/a/text()').extract_first()
            # 商品类型
            catalogue = response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[1]/text()').extract_first()
            # 商品人气值或者交易量
            popularity = response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[2]/text()').extract_first()
            # 商品日期
            date = response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[3]/text()').extract_first()
            # 商品图片地址
            image = response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/a/img/@src').extract_first()
            #输出商品视频地址
            print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[4]/a/@href').extract_first())
            #拼接商品视频网络地址
            video_arr = str(response.xpath(
                '//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[4]/a/@href').extract_first()).split('/')
            video = 'http://172.86.93.240:6534/index.php/vodplay/' + video_arr[3] + '-1-1/'
            #写入数据到变量
            self.values = (
            str(uuid.uuid1()), str(name), str(catalogue), str(popularity), str(date), str(image), str(video))
            self.is_connected() #确定数据库处于连接状态
            self.to_connect() #再次连接数据库
            print(self.query1)#输出要保存数据的SQl语句
            print(self.values)#输出要保存的数据
            self.cursor.execute(self.query1, self.values) #数据库游标存储数据信息
            self.db.commit() #提交事物
            self.count1 = self.count1 + 1 #计数器
            print(self.count1) #输出爬取数据序号最后统计总数

    # 加载每个菜单列表主页面,获取总页数信息,并迭代每一页
    def link(self, response):
        print('当前页面网址')
        print(response.meta['link'])
        #获取网站的总页码
        total = response.xpath('//*[@id="jq"]/div[2]/div[2]/a[12]/text()').extract_first()#爬取总页码元素
        total = str(total).split('/')#分离出页码
        print(total)#输出页码信息
        for b in range(1,(int(total[1])//14)+1):#循环迭代总页码,遍历每一页进行爬取信息
            # time.sleep(2) # 这里的是两秒 爬取过快页面刷新不出来,
            url1 = str(response.meta['link']).strip('/')+'-' + str(b) + '/' #循环拼接每一页地址信息
            print(url1)
            self.count = self.count + 1 #页数计数器
            yield scrapy.Request(url1, headers=self.headers, dont_filter=True, callback=self.list) #翻页请求并执行页面内循环方法

    # 加载网站主页面,获取菜单列表的链接信息
    def parse(self, response):
        # 循环遍历网站菜单列表
        for x in range(2,4):
            #根据列表自定义内容,根据实际情况调整爬取部分
            if x==2:
                # 每一行循环遍历菜单列表
                for i1 in range(2, 11):
                    # 获取列表菜单中链接相关信息
                    link = response.xpath('/html/body/div[2]/div['+str(x)+']/div/ul/li['+str(i1)+']/a/@href').extract_first()
                    # 拼接真正的菜单链接地址
                    url_link = 'http://172.86.93.240:6534' + link
                    # 输出链接地址查看链接地址是否正确
                    print(url_link)
                    #模拟点击菜单,根据菜单链接发送请求,并把链接以参数传递过去
                    yield scrapy.Request(url_link, headers=self.headers, dont_filter=True,callback=self.link, meta={'link': url_link})
            else:
                # 每一行循环遍历菜单列表
                for i2 in range(2, 7):
                    # 获取列表菜单中链接
                    link = response.xpath(
                        '/html/body/div[2]/div[' + str(x) + ']/div/ul/li[' + str(i2) + ']/a/@href').extract_first()
                    # 拼接真正的菜单链接地址
                    url_link = 'http://172.86.93.240:6534' + link
                    #输出链接地址查看链接地址是否正确
                    print(url_link)
                    #模拟点击菜单,根据菜单链接发送请求,并把链接以参数传递过去
                    yield scrapy.Request(url_link, headers=self.headers, dont_filter=True,callback=self.link, meta={'link': url_link})
        self.db.close()  # 关闭数据库连接必须放在外边
    pass


    # time.sleep(2)
    # /html/body/div[3]/div[2]/div/ul/li[2]/a
    # /html/body/div[3]/div[3]/div/ul/li[2]/a
    # /html/body/div[2]/div[2]/div/ul/li[2]/a
    # /html/body/div[2]/div[3]/div/ul/li[2]/a
    # url = 'http://172.86.93.240:6534/index.php/vodtype/0CCCCS/'

    # /html/body/div[3]/div[2]/div/ul/li[2]/a
    # /html/body/div[3]/div[3]/div/ul/li[2]/a
    # http://172.86.93.240:6534/
    # url = 'http://172.86.93.240:6534/index.php/vodtype/0CCCCS/'

    # yield scrapy.Request(link, headers=self.headers, dont_filter=True, callback=self.link)  # 翻页请求并执行页面内循环

    # #获取网站的总页码
    # total = response.xpath('//*[@id="jq"]/div[2]/div[2]/a[12]/text()').extract_first()#爬取总页码元素
    # total = str(total).split('/')#分离出页码
    # print(total)#输出页码信息
    # for b in range(1,int(total[1])+1):#循环迭代总页码,遍历每一页进行爬取信息
    #     time.sleep(2) # 这里的是两秒 爬取过快页面刷新不出来,
    #     url1 = 'http://172.86.93.240:6534/index.php/vodtype/0CCCCS-' + str(self.count) + '/' #拼接下一页地址
    #     print(url1)
    #     self.count = self.count + 1 #页数计数器
    #     yield scrapy.Request(url1, headers=self.headers, dont_filter=True, callback=self.list) #翻页请求并执行页面内循环方法
    # self.db.close()#关闭数据库连接必须放在外边

    # 迭代读取每行数据
    # values中元素有个类型的强制转换，否则会出错的
    # 应该会有其他更合适的方式，可以进一步了解

    # allowed_domains = ['guochanqingse.com']
    # http://172.86.93.240:6534/index.php/vodtype/0CCCCS/
    #
    # Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
    # Accept-Encoding:gzip, deflate
    # Accept-Language:zh-CN,zh;q=0.9
    # Connection:keep-alive
    # Cookie:UM_distinctid=17c025388b5202-00a09d0bdf3ff9-b7a103e-100200-17c025388b62e9; CNZZDATA1279979848=1590887380-1632124990-null%7C1632124990
    # Host:172.86.93.240:6534
    # Referer:http://172.86.93.240:6534/index.php/vodtype/0CCCCS-10/
    # Upgrade-Insecure-Requests:1
    # User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36
    #
    # start_urls = ['http://172.86.93.240:6534/index.php/vodtype/0CCCCS/']

# // *[ @ id = "jq"] / div[2] / div[2] / a[12]

# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul').extract_first())

# print(response.xpath('//*[@id="jq"]/div[2]/div[2]/a[12]/text()').extract_first())
# //*[@id="jq"]/div[2]/div[1]/ul
# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/h2/a/text()').extract_first())
# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[1]/text()').extract_first())
# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[2]/text()').extract_first())
# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/p[3]/text()').extract_first())
# print(response.xpath('//*[@id="jq"]/div[2]/div[1]/ul/li[' + str(c) + ']/a/img/@src').extract_first())
# //*[@id="jq"]/div[2]/div[1]/ul/li[12]/p[4]/a

# //*[@id="jq"]/div[2]/div[1]/ul/li[1]/p[1]

# self.execute_db(self.query1, self.values)
#     self.db.commit()
# self.cursor.close()
# yield scrapy.Request(video, callback=self.video)

# for b in range(1,int((str(response.xpath('//*[@id="jq"]/div[2]/div[2]/a[12]/text()').extract_first()).split('/'))[1])+1):

# try:
#     # 名称 分类 人气 时间 图片 视频
#     cursor.execute('create table '
#                    'dabolu1_list('
#                    'NO int primary key,'#主键
#                    'name VARCHAR,'#名称
#                    'catalogue VARCHAR,'#分类
#                    'popularity VARCHAR,'#人气
#                    'date datetime, '#时间
#                    'image VARCHAR, '#图片
#                    'video VARCHAR '#视频
#                    ')')
# except:
#     print('数据库已存在！')


# self.crawler.engine.close_spider(self, "当调用此方法时打印信息为：无有效信息，关闭spider")

# for r in range(0, len(data)):
#     num = data.ix[r, 0]
#     date = data.ix[r, 1]
#     sale = data.ix[r, 2]
#     values = (int(num), str(date), float(sale))
#     cursor.execute(query, values)

# 关闭游标，提交，关闭数据库连接
# 如果没有这些关闭操作，执行后在数据库中查看不到数据

# # 重新建立数据库连接
# db = pymysql.connect(host='127.0.0.1',user= 'root',password= '123456', db='dabolu1')
# cursor = db.cursor()
# # 查询数据库并打印内容
# cursor.execute('''select * from dabolu1_list''')
# results = cursor.fetchall()
# for row in results:
#     print(row)
# # 关闭
# self.cursor.close()
# self.db.commit()
# self.db.close()
# http://172.86.93.240:6534/index.php/vodtype/0CCCCS-2/
# //*[@id="jq"]/div[2]/div[2]/a[12]

# //*[@id="jq"]/div[2]/div[1]/ul/li/a
# //*[@id="jq"]/div[2]/div[1]/ul/li/p[3]
# //*[@id="jq"]/div[2]/div[1]/ul/li/h2/a
# //*[@id="jq"]/div[2]/div[1]/ul/li[1]/h2/a
# //*[@id="jq"]/div[2]/div[1]/ul/li[4]/h2/a
# //*[@id="jq"]/div[2]/div[1]/ul/li[1]/p[2]
# //*[@id="jq"]/div[2]/div[1]/ul/li[12]/a/img
# int(total[1])
# http://172.86.93.240:6534/index.php/vodplay/47LCCS-1-1/
# //*[@id="jq"]/div[2]/div[1]/ul/li[1]/p[4]/a
# //*[@id="player1"]/div[2]/video
# http://172.86.93.240:6534/index.php/vodplay/tULCCS-1-1/
# //*[@id="player1"]/div[2]/videol
