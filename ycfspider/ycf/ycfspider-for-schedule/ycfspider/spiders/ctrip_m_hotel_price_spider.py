#encoding=utf-8
import base64
import random
import datetime
import json
import sys
import re
import traceback
import os

from scrapy_redis.spiders import RedisSpider

from scrapy.exceptions import DontCloseSpider
from scrapy.http import Request
from scrapy.conf import settings


# from twisted.internet import reactor
from ycfspider.utils.enum import *
from ycfspider.utils.useragent import *
from ycfspider.items import YcfspiderItem
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord
__author__ = 'lizhipeng'

reload(sys)
sys.setdefaultencoding('utf-8')
# reactor.suggestThreadPoolSize(15)


class CtripMHotelPriceSpider(RedisSpider):
    name = 'CtripMHotelPriceSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:ctrip_m_hotel_price_all'
    else:
        redis_key = 'spider:ctrip_m_hotel_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(CtripMHotelPriceSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def next_request(self):
        if self.server.llen(self.name+':requests')==0:
            item = self.server.lpop(self.redis_key)
            if item:
                return item

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def schedule_next_request(self):
        item = self.next_request()
        if item:
            item = eval(item)
            hotel_id = item['hotel_id']
            # hotel_id = '1785884'
            hotel_name = item['hotel_name']
            # log.msg('scrapy info:'+' '+hotel_id+' '+hotel_name, level=log.INFO, spider = 'ctrip_sprider')
            today = datetime.datetime.now()
            crawl_date = settings.get('CTRIP_CRAWL_DATE', 7)
            for delta_day in range(0, crawl_date):
                # print delta_day
                check_in_date = (today+datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
                # print check_in_date
                check_out_date = (today+datetime.timedelta(days=delta_day+1)).strftime('%Y-%m-%d')
                # print check_out_date
                taget_url = 'http://m.ctrip.com/restapi/soa2/10933/hotel/product/roomgetv2?_fxpcqlniredt=09031047110154058410'
                headers = {
                    'Host': 'm.ctrip.com',
                    'Accept-Encoding':'gzip, deflate',
                    'User-Agent': random.choice(user_agent_list),
                    'Referer': 'http://m.ctrip.com/webapp/hotel/hoteldetail/%s.html?days=1&atime=%s&contrl=2&num=1&biz=1' % (hotel_id, check_in_date.replace('-', ''))
                    # 'Cookie': '_abtest_userid=ff6b5405-ecac-4a72-85de-f1bbc767e2ed; ASP.NET_SessionSvc=MTAuMTUuMTM2LjM3fDkwOTB8b3V5YW5nfGRlZmF1bHR8MTQ0OTA1MDAzNzY5NA; appFloatCnt=1; manualclose=1; _fpacid=09031071410315827182; GUID=09031071410315827182; ASP.NET_SessionId=um5ena2fqysjmqlz4fc5wzrs; adscityen=Guangzhou; HotelCityID=32split%E5%B9%BF%E5%B7%9EsplitGuangzhousplit2016-07-26split2016-07-27split0; __zpspc=9.3.1469518887.1469520625.4%234%7C%7C%7C%7C%7C%23; _jzqco=%7C%7C%7C%7C1469503451483%7C1.1130076745.1469503451147.1469522322085.1469524619911.1469522322085.1469524619911.undefined.0.0.17.17; _ga=GA1.2.1480810611.1469503449; _gat=1; _bfa=1.1469503144194.b3fuk.1.1469518733778.1469524619735.4.23; _bfs=1.2; _bfi=p1%3D212094%26p2%3D212093%26v1%3D23%26v2%3D22'
                }
                cookie_str = '_fpacid=09031053410341774521; _bfa=1.1472460409859.9g6wn.1.1472460409859.1472460409859.1.1; _bfs=1.1; GUID=09031053410341774521; _jzqco=%7C%7C%7C%7C%7C1.365063071.1472460409969.1472460409969.1472460409970.1472460409969.1472460409970.0.0.0.1.1; _bfi=p1%3D212094%26p2%3D0%26v1%3D1%26v2%3D0'
                cookie_split = cookie_str.split(';')
                cookie_dic = {}
                for cookie_item in cookie_split:
                    item_split = cookie_item.split('=', 1)
                    cookie_dic[item_split[0].lstrip()] = item_split[1]
                # guid = cookie_dic['GUID']
                guid = '09031053410341774521'
                # cookie_dic ={
                #     '_abtest_userid': '659e8f34-bfe8-4625-a56f-14800d3976fd',
                #     'GUID': guid,
                #     '__utma': '1.1089338571.1446516416.1467872978.1467872978.1',
                #     '__utmz': '1.1467872978.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
                #     '__zpspc': '9.6.1467940597.1467940597.1%231%7Cbaidu%7Ccpc%7Cbaidu81%7C%25E6%2590%25BA%25E7%25A8%258B%7C%23',
                #     'ASP.NET_SessionId': 'z4kogbi5gtepxtd2cxjhx5iq',
                #     'NSC_WT_Xbq_80': 'ffffffff090025c145525d5f4f58455e445a4a423660',
                #     '_gat': '1',
                #     '_jzqco': '%7C%7C%7C%7C1470128046322%7C1.472715767.1446516416452.1471403317542.1471403318180.1471403317542.1471403318180.undefined.0.0.114.114',
                #     '_ga': 'GA1.2.1089338571.1446516416',
                #     '_bfa': '1.1446516412956.2ahs8d.1.1470128044797.1471403314463.28.293',
                #     '_bfs': '1.6',
                #     '_bfi': 'p1%3D212094%26p2%3D212093%26v1%3D293%26v2%3D292'
                # }
                values = {
                    'alliance': {"ishybrid": 0},
                    'anony': True,
                    'contentType': 'json',
                    'contrl': '8',
                    'fc': self.get_fc(hotel_id, check_in_date),
                    'flag': '512',
                    'head': {
                        "cid": guid,
                        "ctok": "",
                        "cver": "1.0",
                        "lang": "01",
                        "sid": "8888",
                        "syscode": "09",
                        "auth": None,
                        "extension": [{
                            "name": "pageid",
                            "value": "212094"
                        },
                        {
                            "name": "webp",
                            "value": 1
                        },
                        {
                            "name": "referrer",
                            "value": "http://m.ctrip.com/html5/ "
                        },
                        {
                            "name": "protocal",
                            "value": "http"
                        }]
                    },
                    'id': hotel_id.encode('utf-8'),
                    'inDay': check_in_date,
                    'membertype': '',
                    'num': '1',
                    'outDay': check_out_date,
                    'pay': '0',
                     'sf': '2',
                    'ver': '0'
                }
                meta = {
                    'channel': 'ctrip',
                    'hotel_id': hotel_id,
                    'hotel_name': hotel_name,
                    'check_in_date': check_in_date,
                    'check_out_date': check_out_date,
                    'retry_count': 0,
                    'guid': guid
                }
                req = Request(url=taget_url, meta=meta, method='post', body=json.dumps(values),  headers=headers, cookies=cookie_dic,
                                  callback=self.parse_hotel_info, dont_filter=True, errback = self.download_errback)
                self.crawler.engine.crawl(req, spider=self)

    def parse_hotel_info(self, response):
        try:
            ycf_items = YcfspiderItem()
            body = response.body
            hotel_id = response.meta['hotel_id']
            hotel_name = response.meta['hotel_name']
            check_in_date = response.meta['check_in_date']
            check_out_date = response.meta['check_out_date']
            # log.msg('scrapy info success:'+' '+hotel_id+' '+hotel_name, level=log.INFO, spider='elong_sprider')
            results = {"hotel_id": hotel_id, "room": []}
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            json_get_values = json.loads(body)
            rooms = json_get_values['rooms']
            room_id_record = []
            for room in rooms:
                if room['bid'] in room_id_record:
                    index = room_id_record.index(room['bid'])
                    room_info_dic = results['room'][index]
                else:
                    room_id_record.append(room['bid'])
                    room_info_dic = {}
                    #获取房型名
                    room_info_dic['room_type_name'] = room['bname']
                    #获取房型id
                    room_info_dic['room_type_id'] = str(room['bid'])
                    room_info_dic['channel_id'] = 1
                    room_info_dic['platform_id'] = 3
                    room_info_dic['hotel_id'] = hotel_id
                    room_info_dic['desc'] = ''
                    if 'fclist' in room:
                        for fc in room['fclist']:
                            room_info_dic['desc'] += fc['ides'] + '##'
                    room_info_dic['desc'] = room_info_dic['desc'].rstrip('##')
                    room_info_dic['area'] = ''
                    room_info_dic['floor'] = ''
                    room_info_dic['bed_type'] = ''
                    room_info_dic['bed_size'] = ''
                    room_info_dic['bed_count'] = ''
                    room_info_dic['max_occupancy'] = ''
                    room_info_dic['internet_service'] = ''
                    room_info_dic['internet_type'] = ''
                    room_info_dic['has_window'] = ''
                    room_info_dic['has_own_toilet'] = ''
                    room_info_dic['has_public_toliet'] = ''
                    room_info_dic['has_toiletries'] = ''
                    room_info_dic['has_slippers'] = ''
                    room_info_dic['has_hot_water'] = ''
                    room_info_dic['has_air_conditioning'] = ''
                    room_info_dic['has_fridge'] = ''
                    room_info_dic['has_computer'] = ''
                    room_info_dic['has_tv'] = ''
                    room_info_dic['has_balcony'] = ''
                    room_info_dic['has_kitchen'] = ''
                    room_info_dic['has_bar'] = ''
                    room_info_dic['has_free_ddd'] = ''
                    room_info_dic['has_free_idd'] = ''
                    room_info_dic['has_breakfast'] = ''
                    room_info_dic['booking_website_number'] = ''
                    room_info_dic['confirm_type'] = ''
                    room_info_dic['status'] = '0'
                    room_info_dic['remark'] = ''
                    room_info_dic['crawl_version'] = ''
                    room_info_dic['crawl_time'] = timestamp
                    room_info_dic['reserved_col1'] = ''
                    room_info_dic['reserved_col2'] = ''
                    room_info_dic['reserved_col3'] = ''
                    room_info_dic['reserved_col4'] = ''
                    room_info_dic['reserved_col5'] = ''
                    #获取房型图片
                    room_info_dic['homepage_picture_url'] = ''
                    if 'url' in room:
                        room_info_dic['homepage_picture_url'] = room['url']
                    room_info_dic['picture_list_url'] = ''
                    if 'images' in room:
                        for image in room['images']:
                            room_info_dic['picture_list_url'] += image['surl']+'##'
                    room_info_dic['picture_list_url'] = room_info_dic['picture_list_url'].rstrip('##')
                    room_info_dic['sub_rooms'] = []
                    results['room'].append(room_info_dic)

                sub_room_info_dic = {}
                #获取子房型id
                sub_room_info_dic['channel_id'] = 1
                #代理商ID
                sub_room_info_dic['id'] = room['sroominfo']['id']
                sub_room_info_dic['product_id'] = str(room['id']) + '_' + str(sub_room_info_dic['id'])
                sub_room_info_dic['platform_id'] = 3
                #获取子房型名
                sub_room_info_dic['product_name'] = room['name']
                # 酒店url
                sub_room_info_dic['product_url'] = 'http://m.ctrip.com/webapp/hotel/hoteldetail/%s.html?days=1&atime=%s' % \
                                                   (hotel_id, check_in_date.replace('-', ''))
                sub_room_info_dic['reserved_col1'] = 'http://hotels.ctrip.com/hotel/%s.html?startdate=%s&depdate=%s' % (hotel_id, check_in_date, check_out_date)
                sub_room_info_dic['hotel_id'] = hotel_id
                sub_room_info_dic['room_type_id'] = room_info_dic['room_type_id']
                sub_room_info_dic['agent_name'] = ''
                if 'agent' in  room['sroominfo']:
                    sub_room_info_dic['agent_name'] = room['sroominfo']['agent']
                if room['guarantee']['type'] == 1:
                    sub_room_info_dic['product_type'] = u'担保'
                else:
                    sub_room_info_dic['product_type'] = u'预付'
                sub_room_info_dic['check_in_type'] = ''
                sub_room_info_dic['need_guarantee'] = ''
                sub_room_info_dic['series_days'] = ''
                sub_room_info_dic['breakfast_type'] = ''
                sub_room_info_dic['cancel_policy'] = ''
                sub_room_info_dic['agent_id'] = sub_room_info_dic['id']
                sub_room_info_dic['total_month_orders'] = ''
                sub_room_info_dic['total_history_orders'] = ''
                sub_room_info_dic['reserved_col2'] = ''
                sub_room_info_dic['reserved_col3'] = ''
                sub_room_info_dic['reserved_col4'] = ''
                sub_room_info_dic['reserved_col5'] = ''
                #获取早餐、床型、礼品等信息
                for info in room['basicinfos']:
                        if info['type'] == 1:
                            sub_room_info_dic['breakfast_type'] = info['value']
                        elif info['type'] == 2:
                            if room_info_dic['bed_type'] == '':
                                room_info_dic['bed_type'] = info['value']
                        elif info['type'] == 3:
                            sub_room_info_dic['cancel_policy'] = info['value']
                        elif info['type'] == 8:
                            if room_info_dic['bed_size'] == '':
                                room_info_dic['bed_size'] = info['value']
                        elif info['type'] == 4:
                            if room_info_dic['area'] == '':
                                room_info_dic['area'] = info['value']
                        elif info['type'] == 5:
                            if 'max_occupancy' not in room_info_dic:
                                room_info_dic['max_occupancy'] = re.findall('\d+', info['value'])[0]
                        elif info['type'] == 6:
                            if room_info_dic['floor'] == '':
                                room_info_dic['floor'] = info['value']
                        elif info['type'] == 9:
                            if room_info_dic['internet_service'] == '  ':
                                room_info_dic['internet_service'] = info['value']
                sub_room_info_dic['confirm_type'] = ''
                sub_room_info_dic['gift_card'] = ''
                sub_room_info_dic['other_preferential_desc'] = ''
                discount_list = ['1e4', 10030, 10040, 10060, 10070, 10080, 10090, 10100, 10110, 10170, 10201, 10211, 10221, 10230,
                                 10250, 10260, 10270, 10280, 10290]
                if 'tags' in room:
                    for tag in room['tags']:
                        if tag['tag']['key'] == 10020 or tag['tag']['key'] == 10010:
                            sub_room_info_dic['confirm_type'] = tag['desc']
                        elif tag['tag']['key'] == 10050:
                            sub_room_info_dic['gift_card'] = tag['desc']
                        elif tag['tag']['key'] in discount_list:
                            if 'desc' in tag['tag']:
                                sub_room_info_dic['other_preferential_desc'] += tag['desc'] + '##'
                sub_room_info_dic['other_preferential_desc'] = sub_room_info_dic['other_preferential_desc'].rstrip('##')
                sub_room_info_dic['need_guarantee'] = ''
                sub_room_info_dic['can_add_bed'] = ''
                sub_room_info_dic['pay_type'] = ''
                sub_room_info_dic['use_integral'] = ''
                sub_room_info_dic['use_coupon'] = ''
                sub_room_info_dic['crawl_version'] = ''
                sub_room_info_dic['crawl_time'] = timestamp

                product_price = {}
                product_price['channel_id'] = 1  #渠道id
                product_price['hotel_id'] = hotel_id
                product_price['room_type_id'] = room_info_dic['room_type_id']
                product_price['product_id'] = sub_room_info_dic['product_id']
                product_price['sell_date'] = check_in_date + ' 00:00:00'
                product_price['price'] = str(room['priceinfo']['prices'][0]['pcny'])
                product_price['preferential_price'] = str(room['priceinfo']['prices'][1]['pcny'])
                if room['priceinfo']['cur'] == 'RMB':
                    product_price['currency'] = '1'
                product_price['is_min_price'] = ''
                product_price['store_status'] = ''
                product_price['crawl_version'] = ''
                product_price['crawl_time'] = timestamp
                product_price['platform_id'] = 3
                product_price['stock'] = ''
                product_price['stock_status'] = ''
                product_price['reserved_col1'] = ''
                product_price['reserved_col2'] = ''
                product_price['reserved_col3'] = ''
                product_price['reserved_col4'] = ''
                product_price['reserved_col5'] = ''
                #是否预订
                features = room['roomfeatures']
                product_price['can_book'] = '1'
                f_flag = True
                for f in features:
                    if f['ftype'] == 9:
                        f_flag = False
                        if f['option'] in (1, 2, 3):
                            product_price['can_book'] = '0'
                if f_flag:
                    product_price['can_book'] = '0'
                sub_room_info_dic['product_price'] = product_price
                room_info_dic['sub_rooms'].append(sub_room_info_dic)
            # 记录原始数据
            original_date = {
                'id': response.meta['hotel_id'],
                'timestamp': timestamp,
                'data': body
            }
            # logger_data.info('ctrip_m_hotel_price ' + 'original data:' + json.dumps(original_date).replace('%', '%%'))
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_PRICE_RESOURSE_PATH')
            ycf_items['results'] = results
            yield ycf_items
            # results_str = json.dumps(results)
            # log.msg('ctrip hotel ' + 'data:' + results_str.replace('%', '%%'), level=log.INFO, spider='ctrip_sprider')
            # logger.info('ctrip hotel ' + 'data:' + results_str.replace('%', '%%'))

        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.M
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def get_fc(self, hotel_id, check_in_date):
        check_in_date = check_in_date.replace('-','')
        t = int(hotel_id) + int(check_in_date)
        s = base64.b64encode(str(t))
        k = list(s)
        s = ''
        for i in range(0, len(k)):
            s += k[i]
            if i == 0 or i == 2 or i == 5 or i == 9 or i == 14:
                s += k[len(k) - i - 1]
        return s
    def download_errback(self, e):
        print type(e), repr(e)


    # def close(spider, reason):
    #     # spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)