# coding=utf-8
import sys

from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from ycfspider.items import YcfspiderItem
import re, os
import json, traceback
import copy
import datetime
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
import time
from ycfspider.utils.enum import *
from ycfspider.tables import scenic_info_table_new

class AlitripPcScenicInfoSpider(RedisSpider):
    name = 'AlitripPcScenicInfoSpider'
    start_urls = []
    # pc版请求头
    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:alitrip_pc_scenic_info_all'
    else:
        redis_key = 'spider:alitrip_pc_scenic_info_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'accept-encoding': 'gzip, deflate, sdch',
               'accept-language': 'zh-CN,zh;q=0.8',
               'cache-control': 'max-age=0',
               'cookie': 'cna=uRsWELyWbRUCAXkh0qIw2ry8; ck1=; uc3=sg2=BqPn64cCjA7wpIMYSklk5f20aEsE4eLTlRGpa6Uw38c%3D&nk2=saDbFuV5Cu%2FU8oW6pus%3D&id2=UUjRIpz4eBqRdw%3D%3D&vt3=F8dAS1HtopMvXRigtA4%3D&lg2=Vq8l%2BKCLz3%2F65A%3D%3D; lgc=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; hng=; _tb_token_=rhJNBIK8rz9N; uss=B0fyW6yKG4DtDVw%2BKY3cTYKafev5tnqV4vay7%2F6toHm26rGHcO6rc9Idio4%3D; tracknick=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; cookie2=1c5464647ee57d0cdb483dca8027aa03; skt=b667dc31a98c18b0; t=1139922cd4fa2e2096749748e098a161; tti=0ab26e8afefe0ab26e8acc96626972214t0cb1; l=Ap2do-SqHUE90jBJzPKezojALXOWcdEG; isg=AiAgnyqnY4lN-9-xIzJ4vlzw8S5ISgTzs1RbF5owJzvElca_QjurgtOnW4ru',
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"
               }

    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            url = 'https://s.alitrip.com/scenic/detail.htm?sid=' + str(item["scenic_id"])
            return Request(url, meta={'item': item}, headers=self.headers, dont_filter=True)

    def __init__(self, *args, **kwargs):
        super(AlitripPcScenicInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH') + '/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH') + '/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)


    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
             raise DontCloseSpider

    # 解析景区详情页信息
    def parse(self, response):
        try:
            item = YcfspiderItem()
            data = response.meta.get("item")
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_INFO_RESOURSE_PATH')
            results = {
                "id": "", "scenic_name": "", "scenic_id": "", "channel_id": "", "scenic_used_name": "",
                "platform_id": "", "scenic_belongs": 0, "group_belongs": "", "country_code": "CN",
                "country_name": "", "province_code": "", "province_name": "", "city_code": "", "city_name_cn": "",
                "city_name_gb": "",
                "district_code": "", "district_name": "", "short_address": "", "address": "", "postal_code": "",
                "position_type": u"经纬度",
                "longitude": "", "latitude": "", "telephone": "", "email": "", "fax": "", "url": "", "picture_url": "",
                "picture_list_url": "", "desc": "", "scenic_ad": "", "scenic_notice": "", "type": "", "level": "",
                "score": "", "currency": "", "ticket_price": "", "min_price": "",
                "tips": "", "comment_desc": "", "total_comment_count": "", "good_comment_rate": "",
                "favourite_count": "", "praise_count": "", "share_count": "",
                "opening_hours": "", "policy": "", "is_credit_card_supportted": "",
                "facility": "", "scenic_service": "", "has_parking_lot": "",
                "has_wifi": "", "has_atm": "", "has_restaurant": "", "has_gym": "", "shared_url": "",
                "active_facility": "", "surround_airport": "", "surround_metro": "", "surround_railway_station": "",
                "surround_bus_station": "", "surround_rent_car": "", "surround_scenic": "",
                "surround_hotel": "", "surround_shopping": "", "surround_food": "",
                "surround_entertainment": "", "status": "", "has_invoice": "", "other_welfare": "", "remark": "",
                "keywords": "", "crawl_version": "", 'reserved_col1': '', 'reserved_col2': '',
                'reserved_col3': '', 'reserved_col4': '', 'reserved_col5': '',
                'crawl_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                'avg_comfort_score': '', "avg_food_score": "", "avg_entertainment_score": "", "avg_safety_score": "",
                "avg_scale_score": "", "avg_popularity_score": "", "avg_desc_score": "", "avg_ticket_score": "",
                "guest_impression": ""
            }
            sel = Selector(response)
            #通用信息
            results['scenic_id'] = data["scenic_id"]
            results['scenic_name'] = data["scenic_name"]
            results['level'] = data["level"]
            # results['telephone'] = data.get("telephone")
            results['longitude'] = data["longitude"]
            results['latitude'] = data["latitude"]
            results['scenic_belongs'] = data["scenic_belongs"]
            results['city_name_cn'] = data["city_name_cn"]
            results['address'] = data["address"]
            results["scenic_belongs"] = data["scenic_belongs"]

            results['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            results['platform_id']=PlatformEnum.PC
            results["channel_id"] = ChannelEnum.ALITRIP
            results["url"] = 'https://s.alitrip.com/scenic/detail.htm?sid=' + str(results["scenic_id"])


            #图片信息与队列
            picture_url_xpath = '//*[@class="scenic-img-wrap clearfix"]/script'
            picture_url = sel.xpath(picture_url_xpath).extract()
            if picture_url :
                text = picture_url[0]
                lis = text[113:len(text)-42]
                p_url = lis.split(',')
                for i in range(0,len(p_url)):
                    sssssss = p_url[i].strip()
                    urls = "https://gtd.alicdn.com/bao/uploaded/"+sssssss[0:len(sssssss)-1]
                    results["picture_list_url"] = results["picture_list_url"] + urls +"##"
                results["picture_url"] = "https://gtd.alicdn.com/bao/uploaded/"+p_url[0].strip()
            #景区价格、电话、当月销量、开放时间、主题分类
            scenic_sell_xpath = '//*[@class="pi-price pi-price-lgt"]/text()'
            scenic_sell1_xpath = '//*[@class="scenic-sell-info"]/dl[1]/dd/em/text()'
            scenic_sell2_xpath = '//*[@class="scenic-sell-info"]/dl[2]/dd'
            scenic_sell3_xpath = '//*[@class="scenic-sell-info"]/dl[3]/dd'
            scenic_sell4_xpath = '//*[@class="scenic-sell-info"]/dl[4]/dd'

            scenic_sell = sel.xpath(scenic_sell_xpath).extract()
            scenic_sell1 = sel.xpath(scenic_sell1_xpath).extract()
            scenic_sell2 = sel.xpath(scenic_sell2_xpath).extract()
            scenic_sell3 = sel.xpath(scenic_sell3_xpath).extract()
            scenic_sell4 = sel.xpath(scenic_sell4_xpath).extract()

            #景区价格
            if scenic_sell:
                sell = scenic_sell[0]
            #当月销量：
            if scenic_sell1:
                sellNum = scenic_sell1[0]
            if scenic_sell2:
                results["telephone"] = scenic_sell2[0]
            #开放时间：
            if scenic_sell3:
                openTime = scenic_sell3[0]
            #主题分类：
            if scenic_sell4:
                type = scenic_sell4[0]
                results["type"] = type

            #抓取景区描述
            des_wrap_xpath ='//*[@class="des-wrap"]/p'
            des_wrap = sel.xpath(des_wrap_xpath).extract()
            if des_wrap:
                results["desc"] = des_wrap[0]

            url = 'https://restapi.amap.com/v3/place/around?location='+ str(results['latitude'])+','+str(results['longitude'])+'&s=rsv3' \
                  '&key=5358d1aa58f3dc1ff8e2bfe17121038d&radius=3000&offset=20&page=1&' \
                  'language=zh_cn&callback=jsonp_835908_&platform=JS&logversion=2.0&' \
                  'sdkversion=1.3&appname=https%3A%2F%2Fs.alitrip.com%2Fscenic%2Fdetail.htm' \
                  '%3Fspm%3D181.7395985.1.1111.rJBr22%26sid%3D1735%23traffic&csid=059CA159-5192-457F-B0B0-8E20955B7026' \
                  '&keywords=%E6%99%AF%E7%82%B9'
            yield Request(url,headers=self.headers,meta={"results":results,"item":item},dont_filter=True,callback=self.parse_arround_scenic)

        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ALITRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    #解析周边请求
    def parse_arround_scenic(self,response):
        body = response.body
        res = body[14:len(body)-1]
        json_res = json.loads(res)
        results = response.meta["results"]
        item = response.meta["item"]
        pois = json_res.get("pois")
        surround_scenic = ""
        for poi in pois:
            surround_scenic = surround_scenic +poi.get("name")+"##"
        results["surround_scenic"] = surround_scenic

        url = 'https://restapi.amap.com/v3/place/around?location=' + str(results['latitude']) + ',' + str(
            results['longitude'])+"&s=rsv3&key=5358d1aa58f3dc1ff8e2bfe17121038d&radius=3000&offset=20&page=1&language=zh_cn&callback=jsonp_313745_&platform=JS&logversion=2.0&sdkversion=1.3&appname=https%3A%2F%2Fs.alitrip.com%2Fscenic%2Fdetail.htm%3Fspm%3D181.7395985.1.1111.rJBr22%26sid%3D1735%23traffic&csid=787B4352-9964-463E-8F32-F73C7575C5D1&keywords=%E9%85%92%E5%BA%97&isg=At7eYWeXBeGY6lE70cSeBG5uL3Q6GLRxITpVHYhlPSEAq3-F8CtsKUIN1QBc&isg2=AoWF9Bel9XmF6hgB9HpmBMf3Fcu9NTnI&isg=Av__h06MlN6hUZD86O9vS8eljtOTY6EEeGFUmpHO7K7poB4imbB91jv2lMel"
        yield Request(url, headers=self.headers, meta={"results": results, "item": item}, dont_filter=True,
                      callback=self.parse_arround_hotel)

    def parse_arround_hotel(self,response):
        body = response.body
        res = body[14:len(body) - 1]
        json_res = json.loads(res)
        results = response.meta["results"]
        item = response.meta["item"]
        pois = json_res.get("pois")
        surround_hotel = ""
        for poi in pois:
            surround_hotel = surround_hotel + poi.get("name") + "##"
        results["surround_hotel"] = surround_hotel

        item["results"] = results
        print item
