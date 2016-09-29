#coding=utf-8
import random
import sys
from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
import time,requests,json,re,datetime,copy
from ycfspider.items import YcfspiderItem
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
from scrapy.conf import settings
from scrapy.utils.project import get_project_settings
from ycfspider.tables.ota_hotel_table import ota_hotel_table
from ycfspider.tables.ota_hotel_room_type_table import ota_hotel_room_type_table
from ycfspider.tables.ota_hotel_room_product_table import ota_hotel_room_product_table
from ycfspider.tables.ota_hotel_product_price_table import ota_hotel_product_price_table
from ycfspider.tables.ota_hotel_stock_table import ota_hotel_stock_table
from ycfspider.utils.qunar_hota_login import QunarHotaLogin
import traceback,os
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from ycfspider.utils.useragent import user_agent_list

class QunarOtaPcHotaHotelInfoSpider(RedisSpider):
    name = "QunarOtaPcHotaHotelInfoSpider"
    allowed_domains = ["hota.qunar.com"]
    settings = get_project_settings()
    redis_key =  'spider:qunar_ota_hota_user_pwd_master'
    qunar_hotel_login = QunarHotaLogin()

     #整合修改
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
        super(QunarOtaPcHotaHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    qunar_login_header_2 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'User-Agent':random.choice(user_agent_list),
        'X-Requested-With':' XMLHttpRequest',
        'Host':'hota.qunar.com'
    }
    qunar_login_header_3 = {
        'Origin': 'http://hota.qunar.com',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/json;charset=UTF-8',
        'Host':'hota.qunar.com',
        'Proxy-Connection':'keep-alive',
        'User-Agent':random.choice(user_agent_list),
        'X-Requested-With':'XMLHttpRequest',
    }

    def next_request(self):
        """Schedules a request if available"""
        item = self.server.lpop(self.redis_key)
        if item:
           # print type(eval(item))
            item = eval(item)
            cookie = self.qunar_hotel_login.get_user_cookie(item['user_name']+'_'+item['channel_number'])
            if not cookie:
                cookie = self.qunar_hotel_login.login(item['user_name'],item['password'],item['channel_number'])#[item['user_name']]
                # print cookie
            if cookie:
                 url = 'http://hota.qunar.com/stats/ohtml/announcement/queryAnnouncements'
                 item['cookie'] = eval(cookie)
                 return Request(url,meta = {'cookiejar' : 1,'item':item},callback = self.parse_after_login,dont_filter = True,cookies=eval(cookie),headers=self.qunar_login_header_2)

    def parse_after_login(self,response):
        try:
            #登录完毕
            body = response.body
            # cookie_dict = {}
            formdata = {"distance":1,"page":1,"pageSize":1000,"supplierId":''}
            if 'supplierIdList' in body:
                supplieridlist = re.findall(r'supplierIdList=[[0-9]+',body)

                if supplieridlist:
                    supplierid = supplieridlist[0].replace('supplierIdList=[','')
                    formdata['supplierId'] = supplierid
            if formdata['supplierId']:
                url = 'http://hota.qunar.com/baseinfo/oapi/shotel/search'
                yield Request(url,method='POST',meta = {'cookiejar' : 1,'formdata':formdata,'item':response.meta['item']},body=json.dumps(formdata),callback=self.parse_shotel,dont_filter=True,headers = self.qunar_login_header_3,cookies=response.meta['item']['cookie'])
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['item']['user_name']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_shotel(self,response):
        try:
            #遍历所有的酒店列表页
            body = json.loads(response.body)

            result = ''
            if 'data' in body.keys():
                data = body['data']
                datakeys = data.keys()
                if 'query' in datakeys:
                    query = data['query']
                    if 'page' in query.keys():
                        currentPage = query['page']
                if 'result' in datakeys:
                    result = data['result']

            # #解析每页酒店基本信息等....
                if result:
                    if  'list' in result.keys():
                        list = result['list']
                        if list:
                            for info in list:#遍历获取酒店的后台id
                                info_keys = info.keys()
                                if 'id' in info_keys:
                                    if info['id']:
                                        #酒店基本信息抓取
                                        hotel_info_url = 'http://hota.qunar.com/baseinfo/oapi/shotel/detail/'+str(info['id'])
                                        yield  Request(hotel_info_url,callback=self.parse_hotel_info,dont_filter=True,headers=self.qunar_login_header_3,meta={'cookiejar' : 1,'ota_hotel_id':info['id'],'item':response.meta['item']},cookies = response.meta['item']['cookie'],priority = 50 )

                                         #房型信息抓取
                                        hotel_room_type_url = 'http://hota.qunar.com/baseinfo/oapi/sroom/queryBySHotelId/'+str(info['id'])+'?curPage=1&pageSize=50&_='+ str(int(time.time() * 1000))
                                        yield  Request(hotel_room_type_url,callback=self.parse_hotel_room_type,dont_filter=True,headers=self.qunar_login_header_3,meta={'cookiejar' : 1,'ota_hotel_id':info['id'],'item':response.meta['item']},cookies = response.meta['item']['cookie'],priority=100)

        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['item']['user_name']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_hotel_info(self,response):
        try:
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            usr_pwd_data = response.meta['item']
            if 'errcode'in body.keys():
                ota_hotel_table['channel_id'] = 2
                ota_hotel_table['platform_id'] = 1
                hotel_table_copy = copy.deepcopy(ota_hotel_table)
                hotel_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                hotel_table_copy['ota_bkstg_name'] = 'qunar_hota'
                hotel_table_copy['channel_sub_name'] = usr_pwd_data['channel_name']
                hotel_table_copy['channel_sub_url'] = usr_pwd_data['login_url']
                hotel_table_copy['channel_sub_id'] = usr_pwd_data['channel_id']
                hotel_table_copy['channel_id'] = 2
                hotel_table_copy['hotel_id'] = response.meta['ota_hotel_id']#酒店后台id
                # hotel_table_copy['ota_bkstg_name'] = 'qunar_hota'
                hotel_table_copy['platform_id'] = 1
                if body['errcode'] == 0:
                    if 'data' in body.keys():
                        data = body['data']
                        data_keys = data.keys()
                        if 'hotelSeq' in data_keys:
                            hotel_table_copy['hotel_front_id'] = data['hotelSeq'] #酒店前台id
                        if 'cityCode' in data_keys:
                            hotel_table_copy['city_name_gb'] = data['cityCode']
                        if 'supplierName' in data_keys:
                            hotel_table_copy['group_belongs'] = data['supplierName']
                        if 'status' in data_keys:
                            hotel_table_copy['status'] = data['status']
                        if 'updateTime' in data_keys:
                            hotel_table_copy['status_update_date'] = data['updateTime']
                        if 'propInfo' in data_keys:
                            propInfo = data['propInfo']
                            propInfo_keys = propInfo.keys()
                            if 'name' in propInfo_keys:
                                hotel_table_copy['hotel_name'] = propInfo['name']
                            if 'enName' in propInfo_keys:
                                hotel_table_copy['hotel_name_gb'] = propInfo['enName']
                            if 'address' in propInfo_keys:
                                hotel_table_copy['address'] = propInfo['address']
                            if 'country' in propInfo_keys:
                                hotel_table_copy['country_name'] = propInfo['country']
                            if 'province' in propInfo_keys:
                                hotel_table_copy['province_name'] = propInfo['province']
                            if 'city' in propInfo_keys:
                                hotel_table_copy['city_name_cn'] = propInfo['city']
                            if 'level' in propInfo_keys:
                                level = propInfo['level']
                                if level:
                                    level = level.split('_')
                                    if level:
                                        hotel_table_copy['type'] = level[1]
                                        hotel_table_copy['star'] = level[1]
                    if 'bizInfo' in body.keys():
                        bizInfo =  body['bizInfo']
                        bizInfo_keys = bizInfo.keys()
                        if 'invoiceProvider' in bizInfo_keys:
                            invoiceProvider = bizInfo['invoiceProvider']
                            if invoiceProvider:
                                hotel_table_copy['is_invoice_supportted'] = 1
                                hotel_table_copy['invoice_desc'] = invoiceProvider
                        if 'serviceConfig' in bizInfo_keys:
                            serviceConfig  = bizInfo['serviceConfig']
                            if 'contactConfig' in serviceConfig.keys():
                                contactConfig = serviceConfig['contactConfig']
                                if 'contactList' in contactConfig.keys():
                                    contactList = contactConfig['contactList']
                                    if contactList:
                                        for tel in contactList:
                                            if 'contactNo' in tel.keys():
                                                hotel_table_copy['telephone'] = ota_hotel_table['telephone'] + str(tel['contactNo'])
                                if 'faxList' in contactConfig.keys():
                                    faxList = contactConfig['faxList']
                                    if faxList:
                                        for fax in faxList:
                                            #没有找到fax的例子
                                            if 'faxNo' in fax.keys():
                                                hotel_table_copy['fax'] = ota_hotel_table['fax'] + str(fax['faxNo'])
                            if 'serviceTimeConfig' in serviceConfig.keys():
                                serviceTimeConfig = serviceConfig['serviceTimeConfig']
                                serviceTimeConfig_keys = serviceTimeConfig.keys()
                                if 'orderConfirmBeginTime' and 'orderConfirmEndTime' in serviceTimeConfig_keys:
                                    hotel_table_copy['order_confirm_time'] = str(serviceTimeConfig['orderConfirmBeginTime'])+'——'+str(serviceTimeConfig['orderConfirmEndTime'])
                                if 'customerArrivalBeginTime' and 'customerArrivalEndTime' in serviceTimeConfig_keys:
                                    hotel_table_copy['check_in_time'] = str(serviceTimeConfig['customerArrivalBeginTime']) + '——'+str(serviceTimeConfig['customerArrivalEndTime'])
                if ota_hotel_table['hotel_front_id']:
                     hotel_url = ''
                     l = hotel_table_copy["hotel_front_id"].split('_')
                     if len(l) == 2:
                        hotel_url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0], l[1])
                        hotel_url = hotel_url.replace('\n', '')
                     elif len(l) == 3:
                        hotel_url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0]+"_"+l[1], l[2])
                        hotel_url = hotel_url.replace('\n', '')
                     if hotel_url:
                         ota_hotel_table['url'] = hotel_url
            item = YcfspiderItem()
            item['kafka_url'] =  settings.get('KAFKA_ADDRESS') +  settings.get('KAFKA_OTA_HOTEL_INFO_RESOURSE_PATH')
            item['results'] = hotel_table_copy
            yield item

        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['ota_hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_hotel_room_type(self,response):
        try:
            #解析酒店房型信息,hotel_id有两种，一种后台id，一种前台id
            #房型信息—产品信息—价格信息
            body = json.loads(response.body)
            self.logger_data.info(body)
            #'username':response.meta['user_name'],'item':response.meta['item']
            usr_pwd_data = response.meta['item']
            hits_num = ''

            ota_hotel_room_type_table['channel_id'] = 2
            ota_hotel_room_type_table['platform_id'] = 1
            room_type_list = []
            if 'errcode'in body.keys():
                if body['errcode'] == 0:
                    if 'data' in body.keys():
                        data = body['data']
                        if 'list' in data.keys():
                            list = data['list']
                            if 'results' in list.keys():
                                results = list['results']
                                if results:
                                    for hit in results:
                                       # ota_hotel_room_type_table
                                        room_type_table_copy = copy.deepcopy(ota_hotel_room_type_table)
                                        room_type_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                                        room_type_table_copy['ota_bkstg_name'] = 'qunar_hota'
                                        room_type_table_copy['channel_sub_id'] = int(usr_pwd_data['channel_id'])
                                        hit_keys = hit.keys()
                                        if 'id' in hit_keys:
                                            room_type_table_copy['room_type_id'] = str(hit['id'])
                                        if 'sHotelId' in hit_keys:
                                            room_type_table_copy['hotel_id'] = str(hit['sHotelId'])
                                        if 'status' in hit_keys:
                                            room_type_table_copy['status'] = hit['status']
                                        if 'propInfo' in hit_keys:
                                            propInfo = hit['propInfo']
                                            propInfo_keys = propInfo.keys()
                                            if 'name' in propInfo_keys:
                                                room_type_table_copy['room_type_name'] = propInfo['name']
                                            if 'floor' in propInfo_keys:
                                                room_type_table_copy['floor'] = propInfo['floor']
                                            if 'area' in propInfo_keys:
                                                room_type_table_copy['area'] = propInfo['area']
                                            if 'bedType' in propInfo_keys:
                                                room_type_table_copy['bed_type'] = propInfo['bedType']
                                            if 'broadband' in propInfo_keys:
                                                broadband = propInfo['broadband']
                                                if 'available' in broadband.keys():
                                                    if broadband['available']:
                                                        ota_hotel_room_type_table['has_internet'] = 1
                                                        if 'feeType' in broadband.keys():
                                                                if 'FREE'in broadband['feeType']:
                                                                    room_type_table_copy['internet_service'] = 'C'
                                                                else:
                                                                    room_type_table_copy['internet_service'] = 'B'
                                        if 'bizInfo' in hit_keys:
                                            bizInfo = hit['bizInfo']
                                            if 'addBedType' in bizInfo.keys():
                                                room_type_table_copy['can_add_bed'] = bizInfo['addBedType']
                                        room_type_list.append(room_type_table_copy)
                    if response.meta['ota_hotel_id']:
                        product_info_url = 'http://hota.qunar.com/price/oapi/product/queryProducts'
                        formdata = {"fromDate":"","toDate":"","shotelId":"","bizTypeSet":[]}
                        formdata["toDate"] =str((datetime.datetime.now()+ datetime.timedelta(days=9)).strftime('%Y-%m-%d'))
                        formdata["fromDate"] =str(datetime.datetime.now().strftime('%Y-%m-%d'))
                        formdata["shotelId"] = str(response.meta['ota_hotel_id'])
                        yield Request(product_info_url,callback=self.parse_products_info, body = json.dumps(formdata),method="POST",meta={'cookiejar' : 1,'room_type_list':room_type_list,'ota_hotel_id':response.meta['ota_hotel_id'],'item':usr_pwd_data},dont_filter=True,headers = self.qunar_login_header_3,cookies = response.meta['item']['cookie'],priority =90)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['ota_hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_products_info(self,response):
        try:
            room_type_list = response.meta['room_type_list']
            usr_pwd_data = response.meta['item']
            hotel_id = response.meta['ota_hotel_id']
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            product_list = []

            currencyDesc = ''
            if 'errcode'in body.keys():
                if body['errcode'] == 0:
                    if 'data' in body.keys():
                         data = body['data']
                        # if 'ratePlanBaseInfos' in data.keys():
                        #     ratePlanBaseInfos = data['ratePlanBaseInfos']
                         if 'currencyDesc' in data.keys():
                            currencyDesc = data['currencyDesc']
                         if 'productDetailResponses' in data.keys():
                            productDetailResponses = data['productDetailResponses']
                            if productDetailResponses:
                                for product in productDetailResponses:
                                    if product:
                                        price_list = []
                                        ota_hotel_room_product_table['channel_id'] = 2
                                        ota_hotel_room_product_table['platform_id'] = 1
                                        room_product_table_copy = copy.deepcopy(ota_hotel_room_product_table)
                                        room_product_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                                        room_product_table_copy['ota_bkstg_name'] = 'qunar_hota'
                                        room_product_table_copy['channel_sub_id'] = int(usr_pwd_data['channel_id'])
                                        if hotel_id:
                                            room_product_table_copy['hotel_id'] = str(hotel_id)
                                        if 'productBaseInfo' in product.keys():
                                            productBaseInfo = product['productBaseInfo']
                                            productBaseInfo_keys = productBaseInfo.keys()
                                        if 'productId' in productBaseInfo_keys:
                                            room_product_table_copy['product_id'] = str(productBaseInfo['productId'])
                                        if 'sroomId' in productBaseInfo_keys:
                                            room_product_table_copy['room_type_id'] = str(productBaseInfo['sroomId'])
                                        if 'productName' in productBaseInfo_keys:
                                            room_product_table_copy['product_name'] = productBaseInfo['productName']
                                        #product_name中包含有套餐等信息
                                        if room_product_table_copy['product_name']:
                                            if '门票' in room_product_table_copy['product_name']:
                                                room_product_table_copy['product_combination_type'] = 3
                                            else:
                                                room_product_table_copy['product_combination_type'] = 1
                                        if 'rateplanId' in productBaseInfo_keys:
                                            room_product_table_copy['rate_plan_id'] = str(productBaseInfo['rateplanId'])
                                        if 'payType' in productBaseInfo_keys:
                                            room_product_table_copy['product_type'] = productBaseInfo['payType']
                                        if 'bizTypes' in productBaseInfo_keys:
                                            room_product_table_copy['business_type'] = str(productBaseInfo['bizTypes'])
                                        if 'sRSellUnit' in productBaseInfo_keys:
                                            if productBaseInfo['sRSellUnit'] == '间':
                                                room_product_table_copy['sell_unit'] =1
                                        if 'commissionType' in productBaseInfo_keys:
                                            room_product_table_copy['commission_type'] = productBaseInfo['commissionType']
                                        #breakfast_type,rate_plan_name,rate_plan_status,先获取response的cookie，然后发起requests请求
                                        if room_product_table_copy['rate_plan_id']:
                                            rate_plan_url ='http://hota.qunar.com/price/oapi/rateplan/detail/'+str(room_product_table_copy['rate_plan_id'])
                                            rate_plan =json.loads(requests.post(url = rate_plan_url,headers=self.qunar_login_header_3, timeout=5,cookies= response.request.cookies).content)
                                            if 'errcode' in rate_plan.keys():
                                                if rate_plan['errcode'] == 0:
                                                    if 'data' in rate_plan.keys():
                                                        rate_plan_data = rate_plan['data']
                                                        if 'rateplan' in rate_plan_data.keys():
                                                            rateplan = rate_plan_data['rateplan']
                                                            rateplan_keys = rateplan.keys()
                                                            if 'cnName' in rateplan_keys:
                                                                room_product_table_copy['rate_plan_name'] = rateplan['cnName']
                                                                room_product_table_copy['breakfast_type'] = rateplan['cnName']
                                                            if 'bookingChannelTypeSet' in rateplan_keys:
                                                                room_product_table_copy['sell_channel'] = str(rateplan['bookingChannelTypeSet'])
                                                        if 'bookingRule' in rate_plan_data.keys():
                                                            bookingRule = rate_plan_data['bookingRule']
                                                            bookingRule_keys = bookingRule.keys()
                                                            if 'fromDate' in bookingRule_keys:
                                                                room_product_table_copy['valid_start_time'] = bookingRule['fromDate']
                                                            if 'toDate' in bookingRule_keys:
                                                                room_product_table_copy['valid_end_time'] = bookingRule['toDate']
                                                            if 'transformedPieceList' in bookingRule_keys:
                                                                transformedPieceList = bookingRule['transformedPieceList']
                                                                if transformedPieceList:
                                                                    transformedPieceList = transformedPieceList[0]
                                                                    if 'piece' in transformedPieceList.keys():
                                                                        piece = transformedPieceList['piece']
                                                                        if 'timeRange' in piece.keys():
                                                                            if 'fromTime' in piece['timeRange'].keys():
                                                                                room_product_table_copy['valid_start_time_by_day'] = piece['timeRange']['fromTime']
                                                                            if 'toTime' in piece['timeRange'].keys():
                                                                                room_product_table_copy['valid_end_time_by_day'] = piece['timeRange']['toTime']
                                                                        if 'minDays' in piece.keys():
                                                                            room_product_table_copy['min_book_nights'] = str(piece['minDays'])
                                                                        if 'maxDays' in piece.keys():
                                                                            room_product_table_copy['max_book_nights'] = str(piece['maxDays'])
                                                                        if 'minAmount' in piece.keys():
                                                                            room_product_table_copy['min_book_copies'] = str(piece['minAmount'])
                                                                        if 'maxAmount' in piece.keys():
                                                                            room_product_table_copy['max_book_copies'] = str(piece['maxAmount'])
                                        #解析price
                                        if 'dailyPriceResponseList' in product.keys():
                                            dailyPriceResponseList = product['dailyPriceResponseList']
                                            if dailyPriceResponseList:
                                                for price in dailyPriceResponseList:
                                                    ota_hotel_product_price_table['channel_id'] = 2
                                                    ota_hotel_product_price_table['platform_id'] = 1
                                                    product_price_table_copy = copy.deepcopy(ota_hotel_product_price_table)
                                                    product_price_table_copy['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                                                    product_price_table_copy['ota_bkstg_name'] = 'qunar_hota'
                                                    product_price_table_copy['channel_sub_id'] = usr_pwd_data['channel_id']
                                                    price_keys = price.keys()
                                                    product_price_table_copy['product_id'] = str(room_product_table_copy['product_id'])
                                                    # product_price_table_copy['channel_id'] = room_product_table_copy['channel_id']
                                                    product_price_table_copy['ota_bkstg_name'] = room_product_table_copy['ota_bkstg_name']
                                                    # product_price_table_copy['platform_id'] = room_product_table_copy['platform_id']
                                                    product_price_table_copy['channel_sub_id'] = room_product_table_copy['channel_sub_id']
                                                    product_price_table_copy['hotel_id'] = room_product_table_copy['hotel_id']
                                                    product_price_table_copy['room_type_id'] = str(room_product_table_copy['room_type_id'])
                                                    if 'date' in price_keys:
                                                        if price['date']:
                                                            product_price_table_copy['sell_date'] = price['date']
                                                            if not product_price_table_copy['sell_date']:
                                                                product_price_table_copy['sell_date'] = time.strftime("%Y-%m-%d")
                                                    if 'basePrice' in price_keys:
                                                        if price['basePrice']:
                                                            product_price_table_copy['settlement_price'] = price['basePrice']
                                                    if 'sellPrice' in price_keys:
                                                        if price['sellPrice']:
                                                            product_price_table_copy['sell_price'] = price['sellPrice']
                                                    if 'marketPrice' in price_keys:
                                                        if price['marketPrice']:
                                                            product_price_table_copy['reference_price'] = price['marketPrice']
                                                    if 'cachBackPrice' in price_keys:
                                                        if price['cachBackPrice']:
                                                            product_price_table_copy['cash_back_price'] = price['cachBackPrice']
                                                    if 'commissionPrice' in price_keys:
                                                        if price['commissionPrice']:
                                                            product_price_table_copy['commission'] = price['commissionPrice']
                                                    if currencyDesc == '人民币':
                                                        product_price_table_copy['currency'] = 1
                                                    price_list.append(product_price_table_copy)
                                        if price_list:
                                            #room_product_table_copy['price_list'] = price_list
                                            room_product_table_copy['product_price'] = price_list
                                        else:
                                            #room_product_table_copy['price_list'] = ''
                                            room_product_table_copy['product_price'] = ''
                                        product_list.append(room_product_table_copy)
            #根据room_type_id对每个产品进行重新组装，组装到对应的room_type下面
            item = YcfspiderItem()
            item['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_OTA_HOTEL_PRICE_RESOURSE_PATH')
            roomProductId = {}
            if room_type_list:
                for room_type in room_type_list:
                    if product_list:
                        products_id_list = []
                        roomProductId[str(room_type['room_type_id'])] = ''
                        for products in product_list:
                            if str(room_type['room_type_id']) == str(products['room_type_id']):
                                products_id_list.append(products['product_id'])
                               # room_type['product_list'].append(products)
                                room_type['sub_rooms'].append(products)
                        roomProductId[str(room_type['room_type_id'])] = products_id_list
                    result_dict = {'room':[]}
                    result_dict['room'].append(room_type)
                    item['results'] = result_dict
                    # print 'room_product_price',item
                    yield item
            #抓取库存状态
            if roomProductId:
                if hotel_id:
                    formdata = {"dateRange":{"fromDate":str(datetime.datetime.now().strftime('%Y-%m-%d')),"toDate":str((datetime.datetime.now()+ datetime.timedelta(days=9)).strftime('%Y-%m-%d'))},
                        "hotelId":str(hotel_id),"roomProductId":roomProductId}
                    store_url = 'http://hota.qunar.com/roomcontrol/oapi/query'
                    yield  Request(store_url,body = json.dumps(formdata),method="POST",meta={'cookiejar' : 1,"roomProductId":roomProductId,'hotel_id':hotel_id,'item':response.meta['item']},callback=self.parse_product_store,dont_filter=True,headers = self.qunar_login_header_3,cookies = response.meta['item']['cookie'],priority =80)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICE
            error_log_dic['id'] = response.meta['ota_hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    #解析库存
    def parse_product_store(self,response):
        try:
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            hotel_id = response.meta['hotel_id']
            usr_pwd_data = response.meta['item']
            roomId = ''
            if 'errcode' in body.keys():
                if body['errcode'] == 0:
                    if 'data' in body.keys():
                        data = body['data']
                        if data:
                            for room_stock in data:
                                room_stock_keys = room_stock.keys()
                                if 'room' in room_stock_keys:
                                    room = room_stock['room']
                                    if room:
                                        if 'roomId' in room.keys():
                                            roomId = room['roomId']
                                        if 'roomControlList' in room.keys():
                                            roomControlList = room['roomControlList']
                                            if roomControlList:
                                                if roomId:
                                                    for stock in roomControlList:
                                                        ota_hotel_stock_table['channel_id'] = 2
                                                        ota_hotel_stock_table['platform_id'] = 1
                                                        hotel_stock_table_copy = copy.deepcopy(ota_hotel_stock_table)
                                                        hotel_stock_table_copy['crawl_time'] =  time.strftime("%Y-%m-%d %H:%M:%S")
                                                        hotel_stock_table_copy['ota_bkstg_name'] = 'qunar_hota'
                                                        hotel_stock_table_copy['channel_sub_id'] = int(usr_pwd_data['channel_id'])
                                                        hotel_stock_table_copy['hotel_id'] = str(hotel_id)
                                                        hotel_stock_table_copy['room_type_id'] = str(roomId)
                                                        hotel_stock_table_copy['total_contract_rooms'] = -1
                                                        stock_keys = stock.keys()
                                                        if 'date' in stock_keys:
                                                            hotel_stock_table_copy['sell_date'] = stock['date']
                                                        if 'buyStatus' in stock_keys:
                                                            if stock['buyStatus'] == 'Y':
                                                                hotel_stock_table_copy['stock_status'] = 1
                                                            else:
                                                                hotel_stock_table_copy['stock_status'] = 3
                                                        if 'limitSaleCount' in stock_keys:
                                                             hotel_stock_table_copy['stock'] = stock['limitSaleCount']
                                                        if 'limitSaleConsume' in stock_keys:
                                                             hotel_stock_table_copy['total_sold'] = stock['limitSaleConsume']
                                                        item = YcfspiderItem()
                                                        item['results'] = hotel_stock_table_copy
                                                        item['kafka_url'] = settings.get('KAFKA_ADDRESS') +  settings.get('KAFKA_OTA_HOTEL_STOCK_RESOURSE_PATH')
                                                        # print item
                                                        yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.OTAHOTELINFOANDPRICEOTAHOTELINFOANDPRICE
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
    #









