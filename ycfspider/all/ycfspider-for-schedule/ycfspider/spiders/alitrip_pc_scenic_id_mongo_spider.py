# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from ycfspider.utils.redisutils import RedisUtil
import re, os
import json, traceback
from pymongo import MongoClient
from pymongo.collection import Collection
import datetime
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
import time
from ycfspider.utils.enum import *


class AlitripPcScenicInfoMongoSpider(RedisSpider):
    name = 'AlitripPcScenicInfoMongoSpider'
    start_urls = []
    # pc版请求头
    headers = {'accept': '*/*',
               'accept-encoding': 'gzip, deflate, sdch',
               'accept-language': 'zh-CN,zh;q=0.8',
               'cache-control': 'max-age=0',
               'cookie':'cna=uRsWELyWbRUCAXkh0qIw2ry8; ck1=; uc3=sg2=BqPn64cCjA7wpIMYSklk5f20aEsE4eLTlRGpa6Uw38c%3D&nk2=saDbFuV5Cu%2FU8oW6pus%3D&id2=UUjRIpz4eBqRdw%3D%3D&vt3=F8dAS1HtopMvXRigtA4%3D&lg2=Vq8l%2BKCLz3%2F65A%3D%3D; lgc=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; hng=; _tb_token_=rhJNBIK8rz9N; uc1=cookie15=URm48syIIVrSKA%3D%3D; uss=B0fyW6yKG4DtDVw%2BKY3cTYKafev5tnqV4vay7%2F6toHm26rGHcO6rc9Idio4%3D; tracknick=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; cookie2=1c5464647ee57d0cdb483dca8027aa03; cookie1=UoLfIa4WSW3rSAKQzbBnddWTXV%2FbX%2FtyF4xp0xN6%2FsU%3D; unb=2000360460; skt=b667dc31a98c18b0; t=1139922cd4fa2e2096749748e098a161; _l_g_=Ug%3D%3D; _nk_=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; cookie17=UUjRIpz4eBqRdw%3D%3D; l=AhAQx7Su-O6w4W3OuTFbMfjTYFRjmfQh; isg=ApGRzHPiokoJxM5iisUZHcW_oJ0oiAVwuiuqZHMnzdh1GrFsu04VQD9wylkH','referer': 'https://s.alitrip.com/scenic/list.htm',
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"
               }
    cookie = {'cna': 'uRsWELyWbRUCAXkh0qIw2ry8',
              'cookie2': '1cec08fdad4f249038dbb1b66aff4bf4',
              't': '1139922cd4fa2e2096749748e098a161',
              '_tb_token_': '0dkzP7gUo0x4fBqoNITb',
              'l': 'AgQE9tFahIrsfflizXWXVz9CVIjyFyiA',
              'isg': 'AiAgn8qvY4srht-xIzJ4vlzw8S6qgwTzs1RbF5o15jvJlcC_QjnUg_bnW4ru'
              }

    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:alitrip_scenic_id'
    else:
        redis_key = 'spider:alitrip_scenic_id'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')
    def __init__(self, *args, **kwargs):
        super(AlitripPcScenicInfoMongoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH') + '/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH') + '/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)
        client = MongoClient(settings.get('MONGODB_HOST'), int(settings.get('MONGODB_PORT')))
        db = client[settings.get('MONGODB_DATABASE')]
        self.collection = Collection(db, 'alitrip_pc_scenic_id_all')
        print ""

    def spider_idle(self):
        self.schedule_next_request()
    def start_requests(self):
        url = 'https://s.alitrip.com/scenic/ajax/list.htm?callback=jsonp852&format=json&q=&stype=&sgrade=&city=&tfee=&tcat=&ttype=&isrefund=false&ismodify=false&istoday=false&seller=&cspuid=&ordert=DEFAULT&orderd=&jumpto=&pagesize=10&moreseller=false&ismorelist=&_input_charset=utf-8'
        # url = 'https://login.taobao.com/member/login.jhtml?redirectURL=https%3A%2F%2Fwww.alitrip.com%2F'
        return [Request(url,dont_filter=True,headers=self.headers,callback=self.parse)]
    # 解析景区详情页信息
    def parse(self, response):
        try:
            body = response.body
            if 'sec' in  body:
                print "登录问题"
            elif  body is  None:
                print ""
            else :
                res = body[32:len(body) - 2]
                json_res = json.loads(res)
                scenicNum = json_res["scenicNum"]
                data = json_res["data"]
                pageSize = json_res["pageSize"]
                total = json_res["total"]
                pageNum = total / pageSize if total % pageSize == 0 else total / pageSize + 1
                results = {
                    "scenic_id":"","scenic_name":"","city_name_cn":"","level":"","address":"","longitude":"","latitude":""
                }
                currentPage = 0
                for currentPage in range(0,pageNum):
                    currentPage += 1
                    url = 'https://s.alitrip.com/scenic/ajax/list.htm?callback=jsonp852&format=json&q=&stype=&sgrade=&city=&tfee=&tcat=&ttype=&isrefund=false&ismodify=false&istoday=false&seller=&cspuid=&ordert=DEFAULT&orderd=&jumpto=' + str(
                        currentPage) + '&pagesize=10&moreseller=false&ismorelist=&_input_charset=utf-8'
                    yield Request(url, meta={"results": results}, headers=self.headers, dont_filter=True,
                                  callback=self.parse_scenic_info)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ALITRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            # error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_scenic_info(self, response):
        body = response.body
        if 'sec' in body:
            print "登录问题"
        elif body is None:
            print ""
        else:
            res = body[32:len(body) - 2]
            json_res = json.loads(res)
            results = response.meta["results"]
            data = json_res["data"]
            # 解析
            for scenic in data:
                results["scenic_id"] = scenic["sid"]
                results["scenic_name"] = scenic["scenicName"]
                results["city_name_cn"] = scenic.get("city")
                results["level"] = scenic["gradeName"]
                results["address"] = scenic["scenicAddr"]
                results["longitude"] = scenic["scenicPosition"][0]
                results["latitude"] = scenic["scenicPosition"][1]
                results["telephone"] = scenic.get("phoneInfo")
                # 国外1国内0
                results["scenic_belongs"] = 1 if scenic["isAbroad"] =="True" else 0
                # scenicImgs = scenic["scenicImgs"]
                # list = []
                # for s in scenicImgs:
                #     s = "https://gtd.alicdn.com/bao/uploaded/" + s
                #     list.append(s)
                #     s = ""
                # results["picture_list_url"] = list
                # results["picture_url"] = list[0]
                print results
                #写入mongo
                self.write_2_mongo(results)


    def write_2_mongo(self, data):
        # 写入mongo，如果失败，重试三次，重试三次失败后放弃，交给下一个写入方式。
        result_flag = False
        do_count = 3
        while do_count:
            try:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.collection.save(data)
                result_flag = True
                print u'写入mongo成功'
                break
            except Exception, e:
                print e
            do_count -= 1
        if not result_flag:
            print u'写入mongo失败'
        return result_flag

