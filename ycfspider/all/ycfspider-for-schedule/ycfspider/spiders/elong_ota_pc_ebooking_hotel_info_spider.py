#coding=utf-8
import random
import sys
from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
import json
global logger_data
global logger_error
from scrapy.http import Request
from ycfspider.tables.ota_hotel_table import ota_hotel_table
from ycfspider.tables.ota_hotel_room_type_table import ota_hotel_room_type_table
from ycfspider.tables.ota_hotel_room_product_table import ota_hotel_room_product_table
from ycfspider.tables.ota_hotel_product_price_table import ota_hotel_product_price_table
from ycfspider.tables.ota_hotel_stock_table import ota_hotel_stock_table
from ycfspider.items import YcfspiderItem
from scrapy.conf import settings
import datetime
import copy
import requests
import time
from ycfspider.utils import elong_ebook_login
import traceback,os
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from ycfspider.utils.useragent import user_agent_list

class ElongOtaPcEbookingHotelInfoSpider(RedisSpider):
    name = 'ElongOtaPcEbookingHotelInfoSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    redis_key = 'spider:elong_ota_ebooking_user_pwd_master'
    ota_hotels_info_url = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_OTA_HOTEL_INFO_RESOURSE_PATH')
    ota_hotels_price_url = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_OTA_HOTEL_PRICE_RESOURSE_PATH')
    ota_hotel_stock_url = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_OTA_HOTEL_STOCK_RESOURSE_PATH')
    headers = {
        'Accept': 'application/json, text/javascript, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'User-Agent': random.choice(user_agent_list),
        'X-Requested-With': ' XMLHttpRequest',
        'Host': 'ebooking.elong.com',
        'Content-Type': 'application/json; charset=UTF-8',
        'Referer': 'http://ebooking.elong.com/ebkauth/login'
    }
    spider_day = 10
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(ElongOtaPcEbookingHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            data = eval(item)
            url = 'http://ebooking.elong.com/ebkcommon/currentHotel/selectHotelList'
            user_name = data['user_name']
            password = data['password']
            cookie = elong_ebook_login.get_user_cookie(user_name + '_' + data['channel_number'])
            if not cookie:
                cookie = elong_ebook_login.login(user_name, password, data['channel_number'])#login_page(user_name, password)
                if cookie:
                    cookie = eval(cookie)
               # self.r.set('%s_%s' % (channel_sub_id, user_name), json.dumps(cookie))
            else:
                cookie = eval(cookie)
            data['cookie'] = cookie
            req = Request(url, method='POST', body='{}', headers=self.headers, cookies=cookie, meta=data, dont_filter=True)
            self.crawler.engine.crawl(req, spider=self)

    def parse(self, response):
        try:
            data = json.loads(response.body)
            self.logger_data.info(json.dumps(data))
            channel_sub_id = response.meta['channel_sub_id']
            channel_sub_name = response.meta['channel_sub_name']
            channel_sub_url = response.meta['channel_sub_url']
            startDate = datetime.datetime.now()
            meta = response.meta
            delta = datetime.timedelta(days=self.spider_day)
            endDate = startDate + delta
            cookie = response.meta['cookie']
            meta['startDate'] = startDate.strftime('%Y-%m-%d')
            meta['endDate'] = endDate.strftime('%Y-%m-%d')
            if int(data['result']['retcode']) == 0:
                for province in data['provinceInfoList']:
                    province_name = province['provinceName']
                    for city in province['cityInfoList']:
                        city_name_cn = city['cityName']
                        for hotel in city['hotelInfoList']:
                            hotel_id = hotel['hotelId']
                            hotel_name = hotel['hotelName']
                            ota_hotel_table['hotel_id'] = hotel_id
                            ota_hotel_table['hotel_name'] = hotel_name
                            ota_hotel_table['channel_id'] = 3
                            ota_hotel_table['ota_bkstg_name'] = 'elong_ebooking'
                            ota_hotel_table['platform_id'] = 1
                            ota_hotel_table['channel_sub_id'] = channel_sub_id
                            ota_hotel_table['channel_sub_name'] = channel_sub_name
                            ota_hotel_table['channel_sub_url'] = channel_sub_url
                            ota_hotel_table['province_name'] = province_name
                            ota_hotel_table['city_name_cn'] = city_name_cn
                            item = YcfspiderItem()
                            item['results'] = ota_hotel_table
                            item['kafka_url'] = self.ota_hotels_info_url
                            yield item
                            url = 'http://ebooking.elong.com/product/roomPrice/ajaxRoomRateList'
                            formData = {'hotelId': hotel_id, 'roomType': "", 'startDate': startDate.strftime('%Y-%m-%d'),
                                        'endDate': endDate.strftime('%Y-%m-%d'), 'productType': "0"}
                            meta['hotel_id'] = hotel_id
                            req = Request(url, method='POST', body=json.dumps(formData), headers=self.headers, cookies=cookie, meta=meta, dont_filter=True,callback=self.parse_product)
                            yield req
            else:
                self.logger_error.error('username：%s 抓取酒店列表失败！')
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['user_name']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_product(self, response):
        try:
            data = json.loads(response.body)
            self.logger_data.info(json.dumps(data))
            channel_sub_id = response.meta['channel_sub_id']
            startDate = response.meta['startDate']
            endDate = response.meta['endDate']
            # 房型list
            hotel_id = response.meta['hotel_id']
            ota_hotel_room_type_table['channel_id'] = 3
            ota_hotel_room_type_table['ota_bkstg_name'] = 'ebooking'
            ota_hotel_room_type_table['platform_id'] = 1
            ota_hotel_room_type_table['channel_sub_id'] = channel_sub_id
            ota_hotel_room_type_table['hotel_id'] = hotel_id
            cookie = response.meta['cookie']
            req = requests.session()
            product_url = 'http://ebooking.elong.com/product/roomPrice/ajaxRoomRate'
            if int(data['retcode']) == 0:
                # 解析房型
                for room in data['data']['roomResponse']:
                    room_type_id = room['roomID']
                    room_type_name = room['roomName']
                    room_type_num = room['roomTypeNum']
                    ota_hotel_room_type_table_copy = copy.deepcopy(ota_hotel_room_type_table)
                    ota_hotel_room_type_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    ota_hotel_room_type_table_copy['room_type_id'] = room_type_id
                    ota_hotel_room_type_table_copy['room_type_name'] = room_type_name
                    ota_hotel_room_type_table_copy['sub_rooms'] = []
                    formData = {'hotelId': hotel_id, 'roomType': room_type_id, 'startDate': startDate, 'endDate': endDate, 'productType': room_type_num}
                    #如果出现bug
                    prodcut_json = req.post(product_url, data=json.dumps(formData), headers=self.headers, cookies=cookie).content
                    if prodcut_json:
                        prodcut_json = json.loads(prodcut_json)
                    # 解析产品
                    ota_hotel_room_product_table['channel_id'] = 3
                    ota_hotel_room_product_table['ota_bkstg_name'] = 'ebooking'
                    ota_hotel_room_product_table['platform_id'] = 1
                    ota_hotel_room_product_table['channel_sub_id'] = channel_sub_id
                    ota_hotel_room_product_table['hotel_id'] = hotel_id
                    ota_hotel_room_product_table['room_type_id'] = room_type_id
                    for rate_plan in prodcut_json['data']['listRatePlan']:
                        ota_hotel_room_product_table_copy = copy.deepcopy(ota_hotel_room_product_table)
                        ota_hotel_room_product_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                        ota_hotel_room_product_table_copy['product_id'] = str(room_type_id) + '_' + str(rate_plan['ratePlanID'])
                        ota_hotel_room_product_table_copy['rate_plan_id'] = rate_plan['ratePlanID']
                        ota_hotel_room_product_table_copy['rate_plan_name'] = rate_plan['cNRatePlanName']
                        ota_hotel_room_product_table_copy['rate_plan_status'] = rate_plan['status']
                        ota_hotel_room_product_table_copy['reserved_col1'] = rate_plan['supplierAbbr']
                        ota_hotel_room_product_table_copy['reserved_col2'] = rate_plan['supplierID']
                        ota_hotel_room_product_table_copy['reserved_col3'] = rate_plan['supplierName']
                        ota_hotel_room_type_table_copy['sub_rooms'].append(ota_hotel_room_product_table_copy)
                        ota_hotel_room_product_table_copy['product_price'] = []
                        # 解析价格
                        ota_hotel_product_price_table['product_id'] = ota_hotel_room_product_table_copy['product_id']
                        f_date = datetime.datetime.strptime(startDate, '%Y-%m-%d')
                        for price in prodcut_json['data']['priceMap'][str(rate_plan['ratePlanID'])]:
                            ota_hotel_product_price_table_copy = copy.deepcopy(ota_hotel_product_price_table)
                            ota_hotel_product_price_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                            ota_hotel_product_price_table_copy['channel_id'] = 3
                            ota_hotel_product_price_table_copy['ota_bkstg_name'] = 'ebooking'
                            ota_hotel_product_price_table_copy['platform_id'] = 1
                            ota_hotel_product_price_table_copy['channel_sub_id'] = channel_sub_id
                            ota_hotel_product_price_table_copy['hotel_id'] = hotel_id
                            ota_hotel_product_price_table_copy['room_type_id'] = room_type_id
                            if price['isNullPrice'] == 0:
                                ltime = time.localtime(long(price['beginDate'])/1000)
                                timeStr = time.strftime("%Y-%m-%d", ltime)
                                ota_hotel_product_price_table_copy['sell_date'] = timeStr
                                ota_hotel_product_price_table_copy['settlement_price'] = price['genSaleCost']
                                ota_hotel_product_price_table_copy['sell_price'] = price['genSalePrice']
                                currencyCode = price['currencyCode']
                                if currencyCode == 'RMB':
                                    ota_hotel_product_price_table_copy['currency'] = 1
                            else:
                                timeStr = f_date.strftime('%Y-%m-%d')
                                f_date = datetime.timedelta(days=1) + f_date
                                ota_hotel_product_price_table_copy['sell_date'] = timeStr
                            ota_hotel_room_product_table_copy['product_price'].append(ota_hotel_product_price_table_copy)
                    item = YcfspiderItem()
                    result_dict = {'room': []}
                    result_dict['room'].append(ota_hotel_room_type_table_copy)
                    item['kafka_url'] = self.ota_hotels_price_url
                    item['results'] = result_dict
                    yield item
            # 设置当前酒店
            set_hotel_url = 'http://ebooking.elong.com/ebkcommon/currentHotel/setCurrentHotel'
            formData = {"hotelId": hotel_id}
            req = Request(set_hotel_url, method='POST', body=json.dumps(formData), headers=self.headers, cookies=cookie, meta=response.meta, dont_filter=True,callback=self.parse_set_hotel)
            yield req

        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def parse_set_hotel(self,response):
        try:
            data = json.loads(response.body)
            self.logger_data.info(json.dumps(data))
            beginDate = response.meta['startDate']
            cookie = response.meta['cookie']
            if data['retcode'] == 0:
                stock_url = 'http://ebooking.elong.com/ebkroom/getAllInventoryForAllToomType'
                formData = {'beginDate': beginDate, 'day': 9}
                s = requests.session()
                body = s.post(stock_url, data=json.dumps(formData), headers=self.headers, cookies=cookie).content
                response._set_body(body)
                self.parse_stock(response)
                # req = Request(stock_url, method='POST', body=json.dumps(formData), headers=self.headers, cookies=cookie,
                #               meta=response.meta, dont_filter=True, callback=self.parse_stock)
                # yield req
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    # 解析库存
    def parse_stock(self, response):
        try:
            channel_sub_id = response.meta['channel_sub_id']
            hotel_id = response.meta['hotel_id']
            data = json.loads(response.body)
            self.logger_data.info(json.dumps(data))
            ota_hotel_stock_table['channel_id'] = 3
            ota_hotel_stock_table['ota_bkstg_name'] = 'ebooking'
            ota_hotel_stock_table['platform_id'] = 1
            ota_hotel_stock_table['channel_sub_id'] = channel_sub_id
            ota_hotel_stock_table['hotel_id'] = hotel_id
            for room_type_name in data['inventoryDetailMap'].keys():
                startDate = response.meta['startDate']
                room_type_id = data['inventoryDetailMap'][room_type_name]['roomId']
                f_date = datetime.datetime.strptime(startDate, '%Y-%m-%d')
                for inventory in data['inventoryDetailMap'][room_type_name]['inventoryDetailList']:
                    ota_hotel_stock_table_copy = copy.deepcopy(ota_hotel_stock_table)
                    ota_hotel_stock_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    ota_hotel_stock_table_copy['room_type_id'] = room_type_id
                    timeStr = f_date.strftime('%Y-%m-%d')
                    f_date = datetime.timedelta(days=1) + f_date
                    ota_hotel_stock_table_copy['sell_date'] = timeStr
                    isHaveInventory = inventory['isHaveInventory']
                    if isHaveInventory:
                        ota_hotel_stock_table_copy['stock_status'] = 1
                    else:
                        ota_hotel_stock_table_copy['stock_status'] = 0
                    ota_hotel_stock_table_copy['stock'] = inventory['savedRoom']['avaliableCount']
                    ota_hotel_stock_table_copy['total_sold'] = inventory['savedRoom']['soldCount']
                    item = YcfspiderItem()
                    item['results'] = ota_hotel_stock_table_copy
                    item['kafka_url'] = self.ota_hotel_stock_url
                    yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)



















