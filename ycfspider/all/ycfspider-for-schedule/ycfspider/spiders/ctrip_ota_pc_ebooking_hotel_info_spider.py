#coding=utf-8
import sys
from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
import requests
import time
import random
from ycfspider.items import YcfspiderItem
import json
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
import datetime
from scrapy.selector import Selector
from scrapy.conf import settings
from scrapy.utils.project import get_project_settings
from ycfspider.tables.ota_hotel_table import ota_hotel_table
from  ycfspider.tables.ota_hotel_room_type_table import ota_hotel_room_type_table
from  ycfspider.tables.ota_hotel_room_product_table import ota_hotel_room_product_table
from  ycfspider.tables.ota_hotel_product_price_table import  ota_hotel_product_price_table
from ycfspider.tables.ota_hotel_stock_table import  ota_hotel_stock_table
from ycfspider.utils.ctrip_ebook_login import CtripEbookLogin
import traceback,os
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from ycfspider.utils.useragent import user_agent_list

class CtripOtaPcEbookingHotelInfoSpider(RedisSpider):
    name = "CtripOtaPcEbookingHotelInfoSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["www.vipdlt.com"]
    settings = get_project_settings()
    ctrip_ebook_login = CtripEbookLogin()
    ctrip_login_header_2 = {
        "User-Agent": random.choice(user_agent_list),
         "Accept-Language": "zh-CN,zh;q=0.8",
         "Accept-Encoding": "gzip, deflate, sdch",
         "Accept": "text/html,applica  tion/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
         # 'Connection': 'keep-alive',
    }
    ctrip_login_header_3 = {
        "User-Agent":  random.choice(user_agent_list),
        "Accept-Language": "zh-CN,zh;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch",
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': 1,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
     }
    redis_key = "spider:ctrip_ota_ebook_user_pwd_master"


    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(CtripOtaPcEbookingHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        requests.session().close()
        ota_user_pwd = self.server.lpop(self.redis_key)
        if ota_user_pwd:
            return ota_user_pwd


    def schedule_next_request(self):
        item = self.next_request()
        if item:
            ota_user_pwd = eval(item)
            cookie = self.ctrip_ebook_login.get_user_cookie(ota_user_pwd['user_name']+'_'+ota_user_pwd['channel_number'])
            if not cookie:
                cookie = self.ctrip_ebook_login.login(ota_user_pwd['user_name'],ota_user_pwd['password'],ota_user_pwd['channel_number'])#[item['user_name']]
            if cookie:
                url = 'http://www.vipdlt.com/MIP/Order/MIP/OrderList.aspx'
                req = Request(url,meta = {'cookiejar' : 1,'username':ota_user_pwd['user_name'],"ota_user_pwd":ota_user_pwd}, #注意这里cookie的获取'proxy_ip':response.meta['proxy_ip'],
                                                 callback = self.parse_page,dont_filter = True,cookies=eval(cookie))
                self.crawler.engine.crawl(req, spider=self)
    def parse_page(self,response):
        try:
            sel = Selector(response)
            current_supplier_xpath = '//input[@id="hfCurrentSupplier"]/@value'
            current_supplier = sel.xpath(current_supplier_xpath).extract()
            current_supplier_string = ''
            if current_supplier:
                for s in current_supplier:
                    current_supplier_string = s
            hotel_table = ota_hotel_table.copy()
            hotel_table['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
            hotel_table["channel_id"] = 1
            hotel_table["ota_bkstg_name"] = "ctrip_dlt"
            hotel_table["platform_id"] = 1
            hotel_table["channel_sub_id"] = int(response.meta["ota_user_pwd"]["channel_id"])
            hotel_table["channel_sub_name"] = str(response.meta["ota_user_pwd"]["channel_name"])
            hotel_table["channel_sub_url"] = str(response.meta["ota_user_pwd"]["login_url"])
            # print cookies_dic
            #获取全部的酒店
            #通过访问第一个页面获取总的酒店数，总分页数
            meta_data = {'cookiejar' : response.meta['cookiejar'],
                         'username':response.meta['username'],
                         # 'proxy_ip':response.meta['proxy_ip'],
                         "hotel_table":hotel_table,
                         "ota_user_pwd":response.meta["ota_user_pwd"],
                         "current_supplier":current_supplier_string}
            v = random.uniform(0,1)
            # print type(str(current_supplier[0]))
            # body_string = 'Method=SearchSupplierHotelListData&Data%5BCityID%5D=0&Data%5BHotelID%5D=&Data%5BHotelName%5D=&Data%5BHotelStatus%5D=-1&Data%5BpageIndex%5D=1&Data%5BpageSize%5D=20&Data%5BIsLoadServiceIndicatorData%5D=True&CurrentSupplier='+str(current_supplier[0])+'&RandomToken='
            body_string = 'Method=SearchSupplierHotelListData&Data%5BCityID%5D=0&Data%5BHotelID%5D=&Data%5BHotelName%5D=&Data%5BHotelStatus%5D=-1&Data%5BpageIndex%5D=' + str(1) + '&Data%5BpageSize%5D=20&Data%5BIsLoadServiceIndicatorData%5D=True&CurrentSupplier=' + str(current_supplier_string)
            url = 'http://www.vipdlt.com/MIP/Hotel/MIP/PPResource/HotelManage.ashx?v='+str(v)
            yield Request(url,headers=self.ctrip_login_header_3,callback=self.parse_hotel_count,meta = meta_data,dont_filter= True,cookies=response.request.cookies,method="POST",body=body_string)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['username']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def parse_hotel_count(self,response):
        try:
            body = json.loads(response.body)
            hotel_table = response.meta["hotel_table"]

            total_page = body["TotalPage"]
            v = random.uniform(0, 1)
            # if total_page == 0:
            #     yield  response.request
            meta_data = {'cookiejar': response.meta['cookiejar'],
                         'username':response.meta['username'],
                         # 'proxy_ip':response.meta['proxy_ip'],
                         "hotel_table":hotel_table,
                         "ota_user_pwd":response.meta["ota_user_pwd"]}
            for i in range(total_page):
                i = i + 1
                body_string = 'Method=SearchSupplierHotelListData&Data%5BCityID%5D=0&Data%5BHotelID%5D=&Data%5BHotelName%5D=&Data%5BHotelStatus%5D=-1&Data%5BpageIndex%5D='+str(i)+'&Data%5BpageSize%5D=20&Data%5BIsLoadServiceIndicatorData%5D=True&CurrentSupplier=' + str(response.meta["current_supplier"])
                url = 'http://www.vipdlt.com/MIP/Hotel/MIP/PPResource/HotelManage.ashx?v=' + str(v)
                yield Request(url, headers=self.ctrip_login_header_3, callback=self.parse_hotels_detail,meta = meta_data, dont_filter=True,cookies=response.request.cookies,method="POST",body=body_string)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['username']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def parse_hotels_detail(self, response):
        try:
            hotel_table = response.meta["hotel_table"]
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            #已修改
            body_data = body.get("Data")
            # hotels_detail_list = []
            for index in range(len(body_data)):
                hotel_table_2 = hotel_table.copy()
                hotel_table_2['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                hotel_table_2["hotel_id"] = str(body_data[index]["HotelID"])
                hotel_table_2["hotel_front_id"] = str(body_data[index]["MasterHotelID"])
                hotel_table_2["hotel_name"] = str(body_data[index]["HotelName"])
                hotel_table_2["hotel_name_gb"] = str(body_data[index]["HotelEname"])
                hotel_table_2["star"] = str(body_data[index]["HotelStar"])
                if body_data[index]["CountryID"] == 1:
                    hotel_table_2["country_name"] = str(u'中国')
                else:
                    hotel_table_2["country_name"] = str(u'其他')
                hotel_table_2["city_name_cn"] = str(body_data[index]["CityName"])
                hotel_table_2["city_name_gb"] = str(body_data[index]["CityEname"].strip())
                hotel_table_2["telephone"] = str(body_data[index]["Tel"])
                hotel_table_2["fax"] = str(body_data[index]["Fax"])
                if  body_data[index]["HotelStatus"] == 1:
                    hotel_table_2["status"] = str(u"在售")
                elif body_data[index]["HotelStatus"] == 0:
                    hotel_table_2["status"]  = str(u"停售")
                elif body_data[index]["HotelStatus"] == 2:
                    hotel_table_2["status"] = str(u"下线")

                else:
                    hotel_table_2["status"] = str(u"其他")
                hotel_table_2["latest_booking_time"] = str(body_data[index].get("ReserveTime"))
                hotel_table_2["url"] = 'http://hotels.ctrip.com/hotel/'+str(body_data[index].get("MasterHotelID"))+'.html'
                # hotel_table_2["has_parking_lot"] = 0
                # hotel_table_2["has_restaurant"] = 0
                # hotel_table_2["has_gym"] = 0

                #返回一个酒店的信息
                # item["hotel_room_type_table"] = hotel_table_2.copy()
                hotel_info_item = YcfspiderItem()
                hotel_info_item['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                    'KAFKA_OTA_HOTEL_INFO_RESOURSE_PATH')
                hotel_info_item['results'] = hotel_table_2
                yield hotel_info_item
                # item = YcfOtaSpiderItem()
                # item["results"] = hotel_table_2
                # yield item
                hotel_item = hotel_table_2.copy()
                v = random.uniform(0, 1)
                hotel_id = hotel_item["hotel_id"]
                url = 'http://www.vipdlt.com/MIP/Room/MIP/PPResource/RoomManage.ashx?v=' + str(v)
                body_string = "Method=GetHotelRooms&Data%5BHotel%5D=" +str( hotel_id) + "&Data%5BRoomStatus%5D=1&Data%5BStartDate%5D=" + time.strftime("%Y-%m-%d")
                yield Request(url, headers=self.ctrip_login_header_3, callback=self.parse_room_type,
                              meta={'cookiejar': response.meta['cookiejar'], 'username': response.meta['username'],
                                   "hotel_item":hotel_item,"ota_user_pwd":response.meta["ota_user_pwd"]}, dont_filter=True,method="POST", body=body_string,cookies = response.request.cookies)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['username']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    # 'proxy_ip': response.meta['proxy_ip'],

    def parse_room_type(self, response):
        try:
            hotel_room_type_list = [] #房型列表
            hotel_item = response.meta["hotel_item"]
            hotel_room_type_table = ota_hotel_room_type_table.copy()
            hotel_room_product_table = ota_hotel_room_product_table.copy()
            hotel_product_price_table = ota_hotel_product_price_table.copy()
            hotel_stock_table = ota_hotel_stock_table.copy()
            #房型初始化
            hotel_room_type_table["channel_id"] = hotel_item["channel_id"]
            hotel_room_type_table["ota_bkstg_name"] = hotel_item["ota_bkstg_name"]
            hotel_room_type_table["platform_id"] = hotel_item["platform_id"]
            hotel_room_type_table["platform_id"] = hotel_item["platform_id"]
            hotel_room_type_table["hotel_id"] = hotel_item["hotel_id"]
            hotel_room_type_table["channel_sub_id"] = hotel_item["channel_sub_id"]
            hotel_room_type_table['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")

            #产品初始化
            hotel_room_product_table["channel_id"] = hotel_item["channel_id"]
            hotel_room_product_table["ota_bkstg_name"] = hotel_item["ota_bkstg_name"]
            hotel_room_product_table["platform_id"] = hotel_item["platform_id"]
            hotel_room_product_table["hotel_id"] = hotel_item["hotel_id"]
            hotel_room_product_table["channel_sub_id"] = hotel_item["channel_sub_id"]
            hotel_room_product_table['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")

            #价格表
            hotel_product_price_table["channel_id"] = hotel_item["channel_id"]
            hotel_product_price_table["ota_bkstg_name"] = hotel_item["ota_bkstg_name"]
            hotel_product_price_table["platform_id"] = hotel_item["platform_id"]
            hotel_product_price_table["hotel_id"] = hotel_item["hotel_id"]
            hotel_room_product_table["channel_sub_id"] = hotel_item["channel_sub_id"]
            hotel_product_price_table['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
            #库存表
            hotel_stock_table["channel_id"] = hotel_item["channel_id"]
            hotel_stock_table["ota_bkstg_name"] = hotel_item["ota_bkstg_name"]
            hotel_stock_table["platform_id"] = hotel_item["platform_id"]
            hotel_stock_table["hotel_id"] = hotel_item["hotel_id"]
            hotel_room_product_table["channel_sub_id"] = hotel_item["channel_sub_id"]
            hotel_stock_table['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")

            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            # if len(body["Data"]) < 1:
            #     pass
            # else:
                # index = 0
            if body.get('Data'):
                for i in range(len(body["Data"])):
                    hotel_room_type_table_2 = hotel_room_type_table.copy()
                    hotel_room_type_table_2["sub_rooms"] = []
                    hotel_room_type_table_2["room_type_id"] = str(body["Data"][i]['BasicRoomId'])
                    hotel_room_type_table_2["room_type_name"] = str(body["Data"][i]['BasicRoomName'])
                    hotel_room_type_table_2["room_type_name"] = str(body["Data"][i]['BasicRoomName'])
                    hotel_room_type_table_2['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    if i > 0:
                        if body["Data"][i]['BasicRoomId'] != body["Data"][i-1]['BasicRoomId']:
                            hotel_room_type_list.append(hotel_room_type_table_2)
                    else:
                        hotel_room_type_list.append(hotel_room_type_table_2)
                for i in range(len(body["Data"])):
                    hotel_room_product_table_2 = hotel_room_product_table.copy()
                    hotel_room_product_table_2["product_price"] = []
                    hotel_room_product_table_2['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")

                   # hotel_room_type_table_2["sub_rooms"].append(str(body['Data'][i]['RoomName']))
                    # 产品表
                    hotel_room_product_table_2['product_id'] = str(body['Data'][i]['RoomId'])
                    hotel_room_product_table_2['product_name'] = str(body['Data'][i]['RoomName'])
                    hotel_room_product_table_2['room_type_id'] = str(body["Data"][i]['BasicRoomId'])
                    hotel_room_product_table_2['breakfast_type'] = str(body['Data'][i]["RoomDynamicList"][0]['BreakfastStr'])
                    hotel_room_product_table_2['product_combination_type'] = "1"
                    for j in range(len(body['Data'][i]["RoomDynamicList"])):
                        hotel_product_price_table_2 = hotel_product_price_table.copy()
                        hotel_stock_table_2 = hotel_stock_table.copy()
                        hotel_product_price_table_2['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                        hotel_stock_table_2['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")

                        # 价格表
                        hotel_product_price_table_2["product_id"] = str(body['Data'][i]['RoomId'])
                        hotel_product_price_table_2["room_type_id"] = str(body["Data"][i]['BasicRoomId'])
                        hotel_product_price_table_2["sell_date"] = str(body['Data'][i]["RoomDynamicList"][j]['EffectDate'])
                        if body["Data"][i]["RoomDynamicList"][0]["Price"] == -1:
                            hotel_product_price_table_2["settlement_price"] = ""
                        else:
                            hotel_product_price_table_2["settlement_price"] = str(body["Data"][i]["RoomDynamicList"][j]["Price"])
                        if body['Data'][i]['Currency'] == u"￥":
                            hotel_product_price_table_2["currency"] = 1
                        # hotel_product_price_table_2["sell_price"] = -1
                        # hotel_product_price_table_2["reference_price"] = -1
                        # hotel_product_price_table_2["commission"] = -1
                        # hotel_product_price_table_2["cash_back_price"] = -1

                        hotel_room_product_table_2['product_price'].append(hotel_product_price_table_2)
                        # 库存表
                        hotel_stock_table_2['stock'] = int(body['Data'][i]["RoomDynamicList"][j]['CanUseQuantity'])
                        hotel_stock_table_2['room_type_id'] = str(body["Data"][i]['BasicRoomId'])
                        # hotel_stock_table_2['total_sold'] = -1
                        # hotel_stock_table_2['stock_status'] = -1
                        hotel_stock_table_2['total_contract_rooms'] = int(body['Data'][i]["RoomDynamicList"][j]['ContractRooms'])
                        hotel_stock_table_2['sell_date'] = str(body['Data'][i]["RoomDynamicList"][j]['EffectDate'])
                        # hotel_stock_table_2['sell_date'] = datetime.datetime.strptime(body['Data'][i]["RoomDynamicList"][j]['EffectDate'], "%Y-%m-%d %H:%M:%S").date()
                        # dtstr = '2014-02-14 21:32:12'
                        stock_item = YcfspiderItem()
                        stock_item['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                            'KAFKA_OTA_HOTEL_STOCK_RESOURSE_PATH')
                        stock_item['results'] = hotel_stock_table_2
                        yield stock_item

                    for r in hotel_room_type_list:
                        if body['Data'][i]['BasicRoomId'] == int(r["room_type_id"]):
                            r["sub_rooms"].append(hotel_room_product_table_2)
                            break
            #yiled 房型表
            for room_type_product in hotel_room_type_list:
                room_product_item = YcfspiderItem()
                room_product_item['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                    'KAFKA_OTA_HOTEL_PRICE_RESOURSE_PATH')
                result_dict = {'room': []}
                result_dict['room'].append(room_type_product)
                room_product_item['results'] = result_dict
                yield room_product_item

                #yield 房型
                # print hotel_room_type_table_2
        except:
            #下次留下来的下载队列
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta["hotel_item"]["hotel_id"]
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    #
    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)
