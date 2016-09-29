#coding=utf-8
import random
import sys
from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
import time
import json,copy
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
import datetime
from scrapy.conf import settings
from scrapy.utils.project import get_project_settings
from ycfspider.items import YcfspiderItem
from ycfspider.tables.ota_hotel_table import ota_hotel_table
from ycfspider.tables.ota_hotel_product_price_table import ota_hotel_product_price_table
from ycfspider.tables.ota_hotel_room_product_table import ota_hotel_room_product_table
from ycfspider.tables.ota_hotel_stock_table import ota_hotel_stock_table
from ycfspider.tables.ota_hotel_room_type_table import ota_hotel_room_type_table
from ycfspider.utils.qunar_zcf_login import QunarZcfLogin
import traceback,os
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from ycfspider.utils.useragent import user_agent_list

class QunarOtaPcZcfHotelInfoSpider(RedisSpider):
    name = 'QunarOtaPcZcfHotelInfoSpider'
    allow_domains = ["cdycf.zcfgoagain.com"]
    settings = get_project_settings()
    redis_key = "spider:qunar_ota_zcf_user_pwd_master"
    qunar_pc_ota_hotel_info_logger = Logger(settings.get('LOG_PATH') + "/" + name + '/original')
    qunar_zcf_login = QunarZcfLogin()

    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(QunarOtaPcZcfHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    qunar_login_header_1 = {
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Host':'cdycf.zcfgoagain.com',
        'Proxy-Connection':' keep-alive',
        'User-Agent': random.choice(user_agent_list),
        'X-Requested-With':' XMLHttpRequest'
    }
    qunar_login_header_2 = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/json;charset=UTF-8',
        'User-Agent': random.choice(user_agent_list),
        'X-Requested-With':' XMLHttpRequest',
        'Host':'cdycf.zcfgoagain.com'
    }
    qunar_login_header_3 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Content-Type': 'application/json;charset=UTF-8',
        'User-Agent': random.choice(user_agent_list),
        'X-Requested-With': ' XMLHttpRequest',
        'Host': 'cdycf.zcfgoagain.com',
    }

    def next_request(self):
        ritem = self.server.lpop(self.redis_key)
        if ritem:
            ritem = eval(ritem)
            cookie = self.qunar_zcf_login.get_user_cookie(ritem['user_name']+'_'+ritem['channel_number'])
            if not cookie:
                cookie = self.qunar_zcf_login.login(ritem['user_name'],ritem['password'],ritem['login_url'],ritem['channel_number'])#[item['user_name']]
                # print cookie
            if cookie:
                values = {"agentName": "","agentIdList": [4459],"grade": "","valid": "","cityCode": "","aHotelName": "","page": 1,"pageSize": 20}
                url = ritem['login_url'] + '/product/api/ahotel/hotelList'
                return Request(url, meta={'cookiejar': 1, 'username': ritem["user_name"],'ritem':ritem},callback=self.parse_hotel_count,
                          dont_filter=True, headers=self.qunar_login_header_3, method='post', body=json.dumps(values),cookies=eval(cookie))


    def parse_hotel_count(self, response):
        try:
            ritem = response.meta['ritem']
            body = json.loads(response.body)
            if (body['errcode'] != 0):
                return
            total_count = body["data"]["result"]["totalCount"]
            page_size = body["data"]["query"]["pageSize"]
            if((int)(total_count)%(int)(page_size) == 0 ):
                 pagetotal = total_count/page_size
            else :
                 pagetotal = total_count/page_size + 1

            for i in range(pagetotal):
                i = i + 1
                url =  ritem['login_url'] + '/product/api/ahotel/hotelList'
                values = {
                    "agentName": "",
                    "agentIdList": [4459],
                    "grade": "",
                    "valid": "",
                    "cityCode": "",
                    "aHotelName": "",
                    "page": i,
                    "pageSize": 20
                }

                yield Request(url, meta={'cookiejar': response.meta['cookiejar'], 'username': ritem["user_name"],'ritem':ritem}, callback=self.parse_hotel_detail,
                                  dont_filter=True, headers=self.qunar_login_header_3, method='post', body=json.dumps(values),cookies=response.request.cookies)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['username']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_hotel_detail(self, response):
        try:
            ritem = response.meta['ritem']
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            if (body['errcode'] != 0):
                return
            results = body['data']['result']['list']
            if len(results) != 0 :
                for result in results:
                    hotel_detail = copy.deepcopy(ota_hotel_table)
                    hotel_detail['channel_sub_name'] = ritem['channel_name']
                    hotel_detail['channel_sub_url'] = ritem['login_url']
                    hotel_detail['channel_id'] = 2
                    hotel_detail['ota_bkstg_name'] =  'qunar_zcf'
                    hotel_detail['platform_id'] = 1
                    hotel_detail['channel_sub_id'] = int(ritem['channel_id'])
                    hotel_detail['channel_sub_name'] = str(ritem['channel_name'])
                    hotel_detail['channel_sub_url'] = str(ritem['login_url'])
                    hotel_id = result['id']
                    hotel_detail['hotel_id'] = str(hotel_id)
                    hotel_detail['address'] = str(result['address'])
                    hotel_detail['hotel_name_gb'] = str(result['ahotelEnName'])
                    hotel_detail['hotel_name'] = str(result['ahotelName'])
                    hotel_detail['city_name_gb'] = str(result['cityCode'])
                    hotel_detail['city_name_cn'] = str(result['cityName'])
                    # hotel_detail[''] = result['createTime']
                    # hotel_detail[''] = result['grade']
                    # hotel_detail[''] = result['hotelSeq']
                    hotel_front_id = result.get('hotelSeq')
                    hotel_detail['hotel_front_id'] = hotel_front_id
                    h = hotel_front_id.split('_')[-1]
                    hotel_detail['url'] = 'http://hotel.qunar.com/city/'+hotel_detail.get('city_name_gb')+'/dt-'+h
                    hotel_detail['telephone'] = str(result.get('phoneNumber'))
                    if(result.get('grade') == 1):
                        hotel_detail['star'] = '经济型'
                    elif (result.get('grade') == 2):
                        hotel_detail['star'] = '三星及舒适'
                    elif(result.get('grade') == 3):
                        hotel_detail['star'] = '四星及高档'
                    elif(result.get('grade') == 4):
                        hotel_detail['star'] = '五星及豪华'
                    elif(result.get('grade') == 5):
                        hotel_detail['star'] = '二星'
                    #supplierVOList
                    supplierVOList = result['supplierVOList']
                    for su in supplierVOList:
                        hotel_detail['country_name'] = str(su['country'])
                        hotel_detail['province_name'] = str(su['province'])
                        supplierId = su['supplierId']
                    hotel_detail['type'] = str(result['types'])
                    hotel_detail['has_parking_lot'] = -1
                    hotel_detail['has_restaurant'] = -1
                    hotel_detail['has_gym'] = -1
                    hotel_detail['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    #酒店信息
                    #放入kafka
                    hotel_info_item = YcfspiderItem()
                    hotel_info_item["kafka_url"] = settings.get("KAFKA_ADDRESS") + settings.get("KAFKA_OTA_HOTEL_INFO_RESOURSE_PATH")
                    hotel_info_item['results'] = hotel_detail
                    yield hotel_info_item

                    values = {
                        'agentIds': [4459],
                        'ahotelId': hotel_id
                    }

                    #产品管理
                    url =  ritem['login_url'] + '/product/api/product/prooms'
                    yield Request(url, meta={'cookiejar': response.meta['cookiejar'], 'username': ritem["user_name"],'hotelid':hotel_id,'ritem':ritem}, callback=self.ota_hotel_room,
                                   dont_filter=True, headers=self.qunar_login_header_3, method='post', body=json.dumps(values),cookies=response.request.cookies)
                    # #库存管理
                    today = datetime.date.today()
                    toDate = today + datetime.timedelta(days=6)

                    url =  ritem['login_url'] + "/rc/api/query?fromDate=" + str(
                        time.strftime("%Y-%m-%d")) + "&toDate=" + toDate.strftime("%Y-%m-%d") + "&ahotelId=" +str(hotel_id)
                    yield Request(url, meta={'cookiejar': response.meta['cookiejar'], 'username': ritem["user_name"], 'hotelid': hotel_id,'ritem':ritem},
                                  callback=self.ota_hotel_stock,dont_filter=True, headers=self.qunar_login_header_3,cookies=response.request.cookies)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['username']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    # #获取酒店房型
    def ota_hotel_room(self,response):
        try:
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            if (body['errcode'] != 0):
                return
            ritem = response.meta['ritem']
            hotel_id = str(response.meta['hotelid'])
            #房型
            #产品
            h = body['data']
            results = h['result']['list']
            #房型，产品
            list_room_type = []
            for result in results :
                res = result['agentHotelRoomVO']
                hotel_room_info = copy.deepcopy(ota_hotel_room_type_table)
                hotel_room_info['hotel_id'] = hotel_id
                hotel_room_info['channel_id'] = 2
                hotel_room_info['ota_bkstg_name'] = 'qunar_zcf'
                hotel_room_info['platform_id'] = 1
                hotel_room_info['channel_sub_id'] = int(ritem['channel_id'])
                hotel_room_info['room_type_id'] = str(res['id'])
                hotel_room_info['room_type_name'] = res['name']
                hotel_room_info['area'] = res['rtArea']
                hotel_room_info['floor'] = res['rtFloor']
                if res['status'] == 1:
                    hotel_room_info['status'] = '在售'
                else:
                    hotel_room_info['status'] = '停售'
                if res['wifi'] == '有':
                    hotel_room_info['has_internet'] = 1
                else:
                    hotel_room_info['has_internet'] = 0
                #n个产品
                res_products = result['products']
                hotel_room_info["sub_rooms"] = []
                for res_product in res_products :
                    ota_hotel_product_info =copy.deepcopy(ota_hotel_room_product_table)
                    ota_hotel_product_info['channel_id'] = 2
                    ota_hotel_product_info['ota_bkstg_name'] = 'qunar_zcf'
                    ota_hotel_product_info['platform_id'] = 1
                    ota_hotel_product_info['channel_sub_id'] = int(ritem['channel_id'])
                    ota_hotel_product_info['product_id'] = str(res_product['id'])
                    ota_hotel_product_info['hotel_id'] = str(res_product['aHotelId'])
                    ota_hotel_product_info['room_type_id'] = str(res_product['aroomId'])
                    ota_hotel_product_info['product_name'] = res_product['productName']
                    ota_hotel_product_info['breakfast_type'] = res_product['breakfast']
                    ota_hotel_product_info['pay_type'] = res_product['payType']
                    ota_hotel_product_info['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    product_desc = ''
                    extraInfos = json.loads(res_product.get('extraInfo'))
                    i = 1
                    str2 = ""
                    for extraInfo in extraInfos:
                        str1 = extraInfo['data']
                        if(str1):
                            str2 = str2 + str(i) +"."+ str1+" "
                            i += 1
                    if '预订' in str2:
                        ota_hotel_product_info['book_desc'] = str2
                    if '房型' in str2:
                        hotel_room_info['desc'] = str2
                    if '港澳台' in str2:
                        ota_hotel_product_info['customer_type'] = 1

                    latest_book_time = res_product.get('bookingItems')[0].get('pieces').get('MONDAY').get('minBeforeCheck')
                    if(latest_book_time):
                        ota_hotel_product_info['latest_book_time'] = str('需提前'+str(latest_book_time.get('preDays')) +'天'+ str(latest_book_time.get('preHours')+'之前'))
                    min_book_nights = res_product.get('bookingItems')[0].get('pieces').get('MONDAY').get('minDays')
                    if(min_book_nights):
                        ota_hotel_product_info['min_book_nights'] = '至少连住'+str(min_book_nights)+'天'
                    ota_hotel_product_info['cancel_policy']= res_product.get('refundItems')[0].get('pieces').get('MONDAY').get('ruleType')

                    hotel_room_info["sub_rooms"].append(ota_hotel_product_info)
                hotel_room_info['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                list_room_type.append(hotel_room_info)

            url =  ritem['login_url'] + "/price/api/price/list"
            today = datetime.date.today()
            reday = today + datetime.timedelta(days=6)
            values = {
                "ahotelId": str(hotel_id),
                "fromDate": today.strftime("%Y-%m-%d"),
                "toDate": reday.strftime("%Y-%m-%d")
            }
            yield Request(url, meta={'cookiejar': response.meta['cookiejar'], 'username': ritem["user_name"],'list_room_type':list_room_type,
                                     'hotelid': hotel_id,'ritem':ritem},
                          callback=self.ota_hotel_price,
                          dont_filter=True, headers=self.qunar_login_header_3, method='post',
                          body=json.dumps(values),cookies=response.request.cookies)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotelid']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def ota_hotel_price(self,response):
        try:
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            if (body['errcode'] != 0):
                return
            pruduct_price_item = YcfspiderItem()
            ritem = response.meta['ritem']
            #存放产品队列
            lists = []
            #存放产品
            list = {
                'product_id':'',
                'product_price':[]
            }
            list_room_type = response.meta['list_room_type']
            hotel_id = response.meta['hotelid']
            prices_data = body['data']['productDetailResponses']
            for price_data in prices_data:
                datas = price_data['dailyPriceResponseList']
                mylist = list.copy()
                mylist['product_id'] = str(price_data['productId'])
                mylist['product_price'] = []
                for dailyPrice in datas:
                    hotel_price = copy.deepcopy(ota_hotel_product_price_table)
                    hotel_price['hotel_id'] = str(hotel_id)
                    hotel_price['channel_id'] = 2
                    hotel_price['ota_bkstg_name'] =  'qunar_zcf'
                    hotel_price['platform_id'] = 1
                    hotel_price['product_id'] = str(price_data['productId'])
                    hotel_price['channel_sub_id'] = int(ritem['channel_id'])
                    hotel_price['sell_date'] = dailyPrice['date']
                    hotel_price['settlement_price'] = dailyPrice['basePrice']['amount']
                    ss = dailyPrice['basePrice']['currency']
                    if (ss == "CNY"):
                        hotel_price['currency'] = 1
                    elif (ss == "USD"):
                        hotel_price['currency'] = 3
                    elif (ss == "HKD"):
                        hotel_price['currency'] = 2
                    elif( ss == "EUR "):
                        hotel_price['currency'] = 4
                    else:
                        hotel_price['currency'] = 0

                    hotel_price['commission'] = ""
                    hotel_price['commission_rate'] = ""
                    hotel_price['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    mylist['product_price'].append(hotel_price)
                lists.append(mylist)
            for rooms in list_room_type:
                product_list = rooms['sub_rooms']
                for product in product_list:
                    product_id = str(product['product_id'])
                    for i in lists:
                        if(product_id == str(i['product_id'])):
                            product['product_price'] =i['product_price']
                            break
                pruduct_price_item['kafka_url'] = settings.get('KAFKA_ADDRESS')+ settings.get('KAFKA_OTA_HOTEL_PRICE_RESOURSE_PATH')
                result_dict={'room':[]}
                result_dict['room'].append(rooms)
                pruduct_price_item['results'] = result_dict
                yield pruduct_price_item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotelid']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    #库存
    def ota_hotel_stock(self,response):
        try:
            ritem = response.meta['ritem']
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            if (body['errcode'] != 0):
                return
            hotel_stock = copy.deepcopy(ota_hotel_stock_table)
            stocks = body['data']
            stock_item = YcfspiderItem()
            hotel_id = response.meta['hotelid']
            roomlist = stocks['roomList']
            hotel_stock['hotel_id'] = str(hotel_id)
            hotel_stock['channel_id'] = 2
            hotel_stock['ota_bkstg_name'] =  'qunar_zcf'
            hotel_stock['platform_id'] = 1
            hotel_stock['channel_sub_id'] = int(ritem['channel_id'])
            for room in roomlist:
                roomControl = room['roomControl']
                hotel_stock['room_type_id'] = str(room['aroomId'])
                for roomc in roomControl:
                    hotel_stock['sell_date'] = roomc['roomDate']
                    hotel_stock['stock_status'] = roomc['saleStatus']
                    hotel_stock['stock'] = roomc.get('remainCount')
                    hotel_stock['total_sold'] = roomc.get('consumeCount')
                    hotel_stock['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                 #   print hotel_stock
                    #放入item
                    stock_item['kafka_url'] = settings.get('KAFKA_ADDRESS')+ settings.get('KAFKA_OTA_HOTEL_STOCK_RESOURSE_PATH')
                    stock_item['results'] = hotel_stock
                 #   print stock_item
                    yield stock_item
            #productlist = stocks['productList']
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['hotelid']
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