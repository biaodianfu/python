#encoding=utf-8
import random
import datetime
import json
import os
import sys
import traceback

import requests
from scrapy_redis.spiders import RedisSpider

from scrapy.http import Request
from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.useragent import *
from ycfspider.items import YcfspiderItem
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import *


# from ycfspider.utils.spider_state_flag_record import spiderStateRecord

__author__ = 'lizhipeng'

reload(sys)
sys.setdefaultencoding('utf-8')

class QunarPcHotelPriceSpider(RedisSpider):

    name = 'QunarPcHotelPriceSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:qunar_pc_hotel_price_all'
    else:
        redis_key = 'spider:qunar_pc_hotel_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(QunarPcHotelPriceSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        if self.server.llen(self.name+':requests')==0:
            item = self.server.lpop(self.redis_key)
            if item:
                return item

    def schedule_next_request(self):
        item = self.next_request()
        if item:
            item = eval(item)
            hotel_id = item['hotel_id']
            hotel_name = item['hotel_name']
            log.msg('scrapy info:'+' '+' '+hotel_id+' '+hotel_name, level=log.INFO, spider = 'qunar_sprider')
            today = datetime.datetime.now()
            crawl_date = settings.get('QUNAR_CRAWL_DATE', 7)
            for delta_day in range(0, crawl_date):
                # print delta_day
                check_in_date = (today+datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
                # print check_in_date
                check_out_date = (today+datetime.timedelta(days=delta_day+1)).strftime('%Y-%m-%d')
                # print check_out_date
                hotel_id_split = hotel_id.split('_')
                city_id = ''
                id_num = hotel_id_split[-1]
                if len(hotel_id_split) > 2:
                    for i in range(0, len(hotel_id_split)-1):
                        city_id = city_id + hotel_id_split[i] + '_'
                    city_id = city_id.rstrip('_')
                else:
                    city_id = hotel_id_split[0]
                taget_url = 'http://te.hotel.qunar.com/render/detailV2.jsp?HotelSEQ=%s&cityurl=%s&fromDate=%s&toDate=%s&basicData=1&lastupdate=-1' % \
                    (hotel_id, city_id, check_in_date, check_out_date)
#                 cookie = 'QN1=O5cLNVd6AgNJ8B25JVNuAg==; _i=RBTKSa-rKGEQX8axs64UCS0ObRmx; _vi=u8uY_qFFxlaejEDq_dbdjPORUekeZtDch\
# 6KD7kUfw6yYvgl1OCqYdljYnzV3VJmI2TmqC1D9FeRN0wKB9XXMhkkndwBkNr80z3MVS5fM6FgpZC6LpCek1tbWWFEINCtWfTQuARD7AeMT-9wZNWoVrq6eObiYTrboVr6o3-oSuGg6\
# ; QN99=541; QunarGlobal=192.168.31.104_45d2c59d_155b8a5d9f6_-3f9f|1467682396209; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3\
# =1467682401,1468231427; QN269=75958950425011E6BD5AC4346BAC1530; QN73=2477-2478; flowidList=.2-1.3-1.4-1\
# .1-3.; pgv_pvi=1541454848; ag_fid=S86w28TOT5KchQiF; __ag_cm_=1468306027099; __utma=183398822.746117920\
# .1467682412.1468306028.1468308037.6; __utmz=183398822.1468308037.6.5.utmcsr=qunar.com|utmccn=(referral\
# )|utmcmd=referral|utmcct=/; _jzqa=1.677284452131642100.1467682412.1468306037.1468308039.6; _jzqx=1.1467682412\
# .1468308039.3.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/guangzhou/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city\
# /guangzhou/; RT_CACLPRICE=1; QN205=cityads6%23mkcdp; csrfToken=JLBaMymdgaldoexpgmlKsanKmJz4Ppbo; QN163\
# =0; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1468306021; PHPSESSID=3jinqgvg4bfckne00232t4s581; QN70=0de885d4b155d969b032\
# ; pgv_si=s6086387712; __utmc=183398822; _jzqc=1; _jzqckmp=1; QN25=60d4b161-eb5d-44f3-a3a9-d7691749c287-9f992f90\
# ; __utmb=183398822.3.10.1468308037; __ads_session=bUp2FgB2wAimuPAnogA=; _jzqb=1.5.10.1468308039.1; QN271\
# =1fa7e7c3-d05c-41c2-bb41-0b750799c0a4; QN42=ogqf2375; _q=U.wskaiwx8424; _t=24581269; _s=s_FUTCIZ4NGKXBOPLMBV4NY2XMCM\
# ; _v=RXOdWaJCyDAswWqHzuIAxK_HGCZRYAtKEp9e3l0_tuowmdRXxFkWcmNXKEQw3L5mgrnWiEP_Rc0jrT8W73N0b4kzc6V_XUDWsJsVk2fa2DuZOPV7zdT5NPJLUUYYimEctOtN-qhusd0ovCsmvPuI_Xg12HGCp5_LePoIohqiX-VR\
# ; QN44=wskaiwx8424; __utmt=1; QN267=1468309783038_ecbf6d4a26bd0b38; QN268=1468309783038_ecbf6d4a26bd0b38\
# |1468309793095_4854618e4183a2fa'
                referer = 'http://hotel.qunar.com/city/%s/dt-%s/?tag=%s' % (city_id, id_num, city_id)
                headers = {
                    'Host': 'te.hotel.qunar.com',
                    'User-Agent': random.choice(user_agent_list),
                    # 'Cookie': cookie,
                    'Accept-Encoding': 'gzip, deflate',
                    'Referer': referer,
                    'Connection': 'keep-alive',
                    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                    'Accept - Language': 'zh - CN, zh;q = 0.8, en;q = 0.6',
                }
                meta = {
                    'channel': 'qunar',
                    'hotel_id': hotel_id,
                    'hotel_name': hotel_name,
                    'check_in_date': check_in_date,
                    'check_out_date': check_out_date,
                    'retry_count': 0,
                    'city_id': city_id,
                    'id_num': id_num
                }
                req = Request(url=taget_url, meta=meta, headers=headers, callback=self.parse_hotel_info, dont_filter=True, errback = self.download_errback)
                # req = FormRequest(url=taget_url, meta=meta, formdata=values,  headers=headers, callback=self.parse_hotel_info, dont_filter=True, errback = self.download_errback)
                self.crawler.engine.crawl(req, spider=self)

    def parse_hotel_info(self, response):
        try:
            now_time = datetime.datetime.now()
            crawl_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
            items = YcfspiderItem()
            body = response.body
            hotel_response_content = body.lstrip('(')
            hotel_response_content = hotel_response_content.rstrip(';')
            hotel_response_content = hotel_response_content.rstrip(')')
            # 记录原始数据
            # logger_data.info('qunar hotel_price ' + 'original data:' + hotel_response_content)
            room_infos_json = json.loads(hotel_response_content, encoding='utf-8')
            hotel_id = response.meta['hotel_id']
            hotel_name = response.meta['hotel_name']
            check_in_date = response.meta['check_in_date']
            check_out_date = response.meta['check_out_date']
            city_id = response.meta['city_id']
            id_num = response.meta['id_num']
            results = {"hotel_id": hotel_id, "room": []}
            rooms_dict = room_infos_json['result']
            room_id_record = []
            for key in rooms_dict.keys():
                room_id = int(rooms_dict[key][12])
                if room_id in room_id_record:
                    index = room_id_record.index(room_id)
                    room_info_dic = results['room'][index]
                else:
                    room_id_record.append(room_id)
                    room_info_dic = {'room_type_id':'','room_type_name':'','platform_id':1,'channel_id':2,'hotel_id':hotel_id,'desc':'',
                                 'area':'','floor':'','bed_type':'','bed_size':'','bed_count':'','homepage_picture_url':'','picture_list_url':'',
                                 'max_occupancy':'','internet_service':'','internet_type':'','has_window':'','has_own_toilet':'','has_public_toliet':'',
                                 'has_toiletries':'','has_slippers':'','has_hot_water':'','has_air_conditioning':'','has_fridge':'','has_computer':'',
                                 'has_tv':'','has_balcony':'','has_kitchen':'','has_bar':'','has_free_ddd':'','has_free_idd':'','has_breakfast':'',
                                 'booking_website_number':'','confirm_type':'','status':'','remark':'','reserved_col1':'','reserved_col2':'',
                                 'reserved_col3': '', 'reserved_col4': '', 'reserved_col5': '', 'crawl_version': '', 'crawl_time': crawl_time}
                    # 获取房型名
                    room_info_dic['room_type_name'] = rooms_dict[key][3]
                    # 获取房型id
                    room_info_dic['room_type_id'] = room_id
                    room_info_dic['platform_id'] = 1
                    room_info_dic['channel_id'] = 2
                    room_info_dic['hotel_id'] = hotel_id
                    room_info_dic['desc'] = ''
                    room_info_dic['area'] = ''
                    room_info_dic['floor'] = ''
                    room_info_dic['bed_type'] = rooms_dict[key][23]
                    room_info_dic['bed_size'] = ''
                    room_info_dic['internet_service'] = ''
                    room_info_dic['confirmtype'] = ''
                    room_info_dic['status'] = 0
                    room_info_dic['remark'] = ''
                    room_info_dic['crawl_time'] = crawl_time
                    room_info_dic['reserved_col1'] = ''
                    room_info_dic['reserved_col2'] = ''
                    room_info_dic['reserved_col3'] = ''
                    room_info_dic['reserved_col4'] = ''
                    room_info_dic['reserved_col5'] = ''
                    room_info_dic['sub_rooms'] = []
                    results['room'].append(room_info_dic)
                min_price = 0
                sub_room_info_dic = {'product_id':'','product_name':'','product_url':'','platform_id':1,'channel_id':2,'hotel_id':hotel_id,'room_type_id':'',
                                 'agent_id':'','agent_name':'','product_type':'','check_in_type':'','is_series':'','series_days':'','breakfast_type':'',
                                 'cancel_policy':'','confirm_type':'','need_guarantee':'','can_add_bed':'','total_month_orders':'','total_history_orders':'',
                                 'pay_type':'','use_integral':'','use_coupon':'','gift_card':'','other_preferential_desc':'','reserved_col1':'','reserved_col2':'',
                                 'reserved_col3': '', 'reserved_col4': '', 'reserved_col5': '', 'crawl_version': '', 'crawl_time': crawl_time}
                sub_room_info_dic['channel_id'] = 2
                sub_room_info_dic['platform_id'] = 1
                sub_room_info_dic['product_id'] = rooms_dict[key][22]
                sub_room_info_dic['product_name'] = rooms_dict[key][2].encode('utf-8')
                sub_room_info_dic['product_url'] = 'http://hotel.qunar.com/city/%s/dt-%s/?fromDate=%s&toDate=%s' %(city_id, id_num, check_in_date, check_out_date)
                sub_room_info_dic['agent_name'] = rooms_dict[key][5]
                sub_room_info_dic['agent_id'] = ''
                room_info_dic['confirm_type'] = ''
                # sub_room_info_dic['need_guarantee'] = int(rooms_dict[key][18])
                sub_room_info_dic['pay_type'] = ''
                sub_room_info_dic['use_integral'] = ''
                sub_room_info_dic['use_coupon'] = ''
                sub_room_info_dic['total_month_orders'] = ''
                sub_room_info_dic['total_history_orders'] = ''
                # 优惠卡
                gift_card = ''
                if rooms_dict[key][35] == 'CAMEL_CARD':
                    gift_card = u'支持使用礼品卡支付房费金额，若发生退款，礼品卡支付部分将即时退回至礼品卡余额账户。'
                if rooms_dict[key][28] != '':
                    if len(gift_card) > 0:
                        gift_card += '##' + self.get_gift_card(rooms_dict[key][6], rooms_dict[key][28], hotel_id, response.meta['proxy'])
                    else:
                        gift_card = self.get_gift_card(rooms_dict[key][6], rooms_dict[key][28], hotel_id, response.meta['proxy'])
                sub_room_info_dic['gift_card'] = gift_card
                sub_room_info_dic['other_preferential_desc'] = ''
                sub_room_info_dic['hotel_id'] = hotel_id
                sub_room_info_dic['room_type_id'] = int(rooms_dict[key][12])
                if rooms_dict[key][14] == 1:
                    sub_room_info_dic['product_type'] = u'预付'
                elif rooms_dict[key][18] == '1':
                    sub_room_info_dic['product_type'] = u'担保'
                else:
                    sub_room_info_dic['product_type'] = ''
                sub_room_info_dic['check_in_type'] = ''
                sub_room_info_dic['breakfast_type'] = rooms_dict[key][25].encode('utf-8')
                if rooms_dict[key][55] == '0':
                    sub_room_info_dic['cancel_policy'] = u'不可取消'
                elif rooms_dict[key][55] == '1':
                    sub_room_info_dic['cancel_policy'] = u'限时取消'
                elif rooms_dict[key][55] == '2':
                    sub_room_info_dic['cancel_policy'] = u'免费取消'
                elif rooms_dict[key][55] == '3':
                    sub_room_info_dic['cancel_policy'] = u'随时退'
                else:
                    sub_room_info_dic['cancel_policy'] = '----'
                sub_room_info_dic['crawl_time'] = crawl_time
                sub_room_info_dic['reserved_col1'] = ''
                sub_room_info_dic['reserved_col2'] = ''
                sub_room_info_dic['reserved_col3'] = ''
                sub_room_info_dic['reserved_col4'] = ''
                sub_room_info_dic['reserved_col5'] = ''
                # 产品价格
                product_price = {'platform_id':1,'channel_id':2,'hotel_id':hotel_id,'room_id':'','product_id':'','sell_date':'','price':'','currency':'',
                             'is_min_price':'','can_book':'','preferential_price':'','stock':'','stock_status':'','reserved_col1':'','reserved_col2':'',
                             'reserved_col3':'','reserved_col4':'','reserved_col5':'','crawl_version':'','crawl_time':crawl_time}
                product_price['channel_id'] = 2
                product_price['platform_id'] = 1
                product_price['hotel_id'] = hotel_id
                product_price['room_id'] = room_id
                product_price['product_id'] = rooms_dict[key][22]
                product_price['sell_date'] = check_in_date + ' 00:00:00'
                product_price['stock'] = ''
                product_price['stock_status'] = ''
                product_price['reserved_col1'] = ''
                product_price['reserved_col2'] = ''
                product_price['reserved_col3'] = ''
                product_price['reserved_col4'] = ''
                product_price['reserved_col5'] = ''
                if int(rooms_dict[key][58]) > 0:
                    product_price['price'] = int(rooms_dict[key][0]) + int(rooms_dict[key][58])
                    sub_room_info_dic['other_preferential_desc'] = u'在线付¥%s立减¥%s' % (
                    product_price['price'], rooms_dict[key][58])
                elif int(rooms_dict[key][59]) > 0:
                    product_price['price'] = int(rooms_dict[key][0]) + int(rooms_dict[key][59])
                    sub_room_info_dic['other_preferential_desc'] = u'到店付¥%s返¥%s' % (
                    product_price['price'], rooms_dict[key][59])
                elif int(rooms_dict[key][60]) < 0:
                    product_price['price'] = int(rooms_dict[key][0]) + int(rooms_dict[key][60])
                    sub_room_info_dic['other_preferential_desc'] = u'¥%s加¥%s税费' % (
                    product_price['price'], str(int(rooms_dict[key][60]) * -1))
                product_price['price'] = int(rooms_dict[key][0]) + int(rooms_dict[key][59]) + int(rooms_dict[key][58])
                product_price['currency'] = 1
                product_price['min_price'] = ''
                if rooms_dict[key][9] == 1:
                    product_price['can_book'] = 1
                    product_price['store_status'] = 1
                else:
                    product_price['can_book'] = 0
                    product_price['store_status'] = 3
                product_price['preferential_price'] = rooms_dict[key][0]
                product_price['crawl_time'] = crawl_time
                sub_room_info_dic['product_price'] = product_price
                room_info_dic['sub_rooms'].append(sub_room_info_dic)
                room_info_dic['booking_website_number'] = len(room_info_dic['sub_rooms'])
                room_info_dic['sub_rooms'].append(sub_room_info_dic)
                room_info_dic['booking_website_number'] = len(room_info_dic['sub_rooms'])
            # 记录原始数据
            original_date = {
                'id': response.meta['hotel_id'],
                'timestamp': crawl_time,
                'data': body
            }
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_PRICE_RESOURSE_PATH')
            items['results'] = results
            yield items
            # results_str = json.dumps(results).decode("unicode_escape")
            # log.msg('qunar hotel ' + 'data:' + results_str.replace('%', '%%'), level=log.INFO, spider='qunar_sprider')
            # logger.info('qunar hotel ' + 'data:' + results_str.replace('%', '%%'))
            # yield items
        except Exception,e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def get_gift_card(self,wrapperid, promotionKey, hotelseq, proxy):
        try:
            url = 'http://hotel.qunar.com/render/pricePromotion.jsp?wrapperid=%s&promotionKey=%s&hotelseq=%s' %(wrapperid, promotionKey, hotelseq)
            proxies = {'http': proxy}
            response = requests.get(url, proxies=proxies, timeout=5)
            promotion = response.json()
            gift_card = ''
            for p in promotion['data']['promotions']:
                gift_card += p['desc'] + '##'
            if len(gift_card) > 0:
                return gift_card[:-2]
            return ''
        except Exception,e:
            log.msg(u'获取礼品信息异常',level=log.ERROR)

    def download_errback(self, e):
        print type(e), repr(e)

    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)