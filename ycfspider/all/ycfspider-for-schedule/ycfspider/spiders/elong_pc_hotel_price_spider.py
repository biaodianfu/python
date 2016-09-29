# encoding=utf-8
import random
import datetime
import json
import sys
import traceback
import os

from scrapy_redis.spiders import RedisSpider

from lxml import etree

from scrapy.http import FormRequest
from ycfspider.items import YcfspiderItem
from scrapy import log
from scrapy.conf import settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.useragent import *
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import *


# from ycfspider.utils.spider_state_flag_record import spiderStateRecord

__author__ = 'lizhipeng'

reload(sys)
sys.setdefaultencoding('utf-8')

class ElongPcHotelPriceSpider(RedisSpider):
    name = 'ElongPcHotelPriceSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:elong_pc_hotel_price_all'
    else:
        redis_key = 'spider:elong_pc_hotel_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        super(ElongPcHotelPriceSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        if self.server.llen(self.name + ':requests') == 0:
            item = self.server.lpop(self.redis_key)
            if item:
                return item

    def schedule_next_request(self):
        item = self.next_request()
        if item:
            item = eval(item)
            hotel_id = item['hotel_id']
            # hotel_id = '32003399'
            hotel_name = item['hotel_name']
            log.msg('scrapy info:' + ' ' + hotel_id + ' ' + hotel_name, level=log.INFO, spider='elong_sprider')
            today = datetime.datetime.now()
            crawl_date = settings.get('ELONG_CRAWL_DATE', 7)
            for delta_day in range(0, crawl_date):
                # print delta_day
                check_in_date = (today + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
                # print check_in_date
                check_out_date = (today + datetime.timedelta(days=delta_day + 1)).strftime('%Y-%m-%d')
                # print check_out_date
                taget_url = "http://hotel.elong.com/ajax/detail/gethotelroomsetjva"
                Referer = "http://hotel.elong.com/" + str(hotel_id) + "/"  # 酒店id要构造.
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'Referer': Referer,
                    'Host': 'hotel.elong.com',
                    'User-Agent': random.choice(user_agent_list),
                }
                cookie_dic = {
                    'CookieGuid': '75b15388-535c-4b53-a61a-f9aa31926477',
                    '_pk_id.2624.42f4': '181146ddba7c1521.1467716133.1.1467716133.1467716133.',
                    'JSESSIONID': '2089C0D98C0EBC128DF1A4046C2F150C',
                    'SessionGuid': 'a92b598a-8d18-4f33-b66e-966eaceff30a',
                    'Esid': 'a0230437-bd6b-4597-8b05-ca0886852987',
                    'newjava2': '2bb6dab387cceff04366286c27f5f14c',
                    '_jzqco': '%7C%7C%7C%7C%7C1.854640055.1452502975626.1467716132308.1471406344134.1467716132308.1471406344134.0.0.0.7.7',
                    'com.eLong.CommonService.OrderFromCookieInfo': 'Status=1&Orderfromtype=2&Isusefparam=0&Pkid=0&Parentid=1000001&Coefficient=0.0&Makecomefrom=0&Cookiesdays=0&Savecookies=0&Priority=9000',
                    'ShHotel': 'OutDate=2016-08-19&InDate=2016-08-18',
                    's_cc': 'true',
                    's_visit': '1',
                    's_sq': '%5B%5BB%5D%5D',
                    '_pk_id.2624.9f06': '09c6d59e9afddc94.1471406345.1.1471406361.1471406345.',
                    '_pk_ses.2624.9f06': '*',
                    'SHBrowseHotel': 'cn=40101627%2C%2C%2C%2C%2C%2C%3B&'
                }
                values = {'detailRequest.bookingChannel': '1', 'detailRequest.cardNo': '192928',
                          'detailRequest.checkInDate': check_in_date + "+00%3A00%3A00",
                          'detailRequest.checkOutDate': check_out_date + "+00%3A00%3A00",
                          'detailRequest.citySeoNameEn': '', 'detailRequest.customerLevel': '1',
                          'detailRequest.hotelIDs': hotel_id, 'detailRequest.isAfterCouponPrice': 'true',
                          'Request.isDebug': 'false', 'detailRequest.isLogin': 'false',
                          'detailRequest.isMobileOnly': 'false',
                          'detailRequest.isNeed5Discount': 'false', 'detailRequest.isTrace': 'false',
                          'detailRequest.language': 'cn', 'detailRequest.needDataFromCache': 'true',
                          'detailRequest.orderFromID': '50793', 'detailRequest.productType': '0',
                          'detailRequest.promotionChannelCode': '0000', 'detailRequest.proxyID': 'ZD',
                          'detailRequest.sellChannel': '1', 'detailRequest.settlementType': '0',
                          'detailRequest.updateOrder': 'false'}

                meta = {
                    'channel': 'elong',
                    'hotel_id': hotel_id,
                    'hotel_name': hotel_name,
                    'check_in_date': check_in_date,
                    'check_out_date': check_out_date,
                    'retry_count': 0,
                }
                req = FormRequest(url=taget_url, meta=meta, formdata=values, cookies=cookie_dic, headers=headers,
                                  callback=self.parse_hotel_info, dont_filter=True, errback=self.download_errback)
                self.crawler.engine.crawl(req, spider=self)

    def parse_hotel_info(self, response):
        try:
            ycf_items = YcfspiderItem()
            body = response.body
            jsonDic = json.loads(body)
            # 记录原始数据
            valueDic = jsonDic["value"]
            html = valueDic["content"]
            productsInfo = valueDic['hotelTipInfo']['productsInfo']

            hotel_id = response.meta['hotel_id']
            hotel_name = response.meta['hotel_name']
            check_in_date = response.meta['check_in_date']
            check_out_date = response.meta['check_out_date']
            # log.msg('scrapy info success:'+' '+hotel_id+' '+hotel_name, level=log.INFO, spider='elong_sprider')
            results = {'hotel_id': hotel_id, 'room': []}
            page = etree.HTML(html)
            htype_list = page.xpath(u"//div[@data-handle='roomType']")
            i = 0
            sub_index = 0
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for item in htype_list:
                i = i + 1
                room_infos = {'sub_rooms': []}
                # 房型字段
                room_infos['channel_id'] = 3
                room_infos['platform_id'] = 1
                room_infos['hotel_id'] = hotel_id
                if not item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_name']/span[@class='l37d']"):
                    room_infos["room_type_name"] = ''
                else:
                    room_infos["room_type_name"] = item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_name']/span[@class='l37d']")[
                        0].text  # 房间类型名
                room_infos['room_type_id'] = item.attrib["data-roomid"]  # 房型id]
                desc_path = u"div[@class='htype_info_list btddd']/table[@class='htype-table']/tbody[1]/tr[@class='ht_tr_other']/td[@class='ht_other']/p"
                if item.xpath(
                        u"div[@class='htype_info_list btddd']/table[@class='htype-table']/tbody[1]/tr[@class='ht_tr_other']/td[@class='ht_other']/p[@class='htype_info_total']"):
                    desc_path = u"div[@class='htype_info_list btddd']/table[@class='htype-table']/tbody[1]/tr[@class='ht_tr_other']/td[@class='ht_other']/p[2]"
                if not item.xpath(desc_path):
                    room_infos["desc"] = ''
                else:
                    room_infos["desc"] = item.xpath(desc_path)[0].text  # 房型描述信息
                if not item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[1]"):
                    room_infos["area"] = ''
                else:
                    room_infos["area"] = item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[1]")[
                        0].text  # 面积信息
                if not item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[7]"):
                    room_infos["floor"] = ''
                else:
                    room_infos["floor"] = item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[7]")[
                        0].text  # 楼层信息
                if not item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[3]"):
                    room_infos["bed_type"] = ''
                else:
                    room_infos["bed_type"] = item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[3]")[
                        0].text  # 床型信息
                room_infos['bed_size'] = ''
                room_infos['bed_count'] = ''
                room_infos['homepage_picture_url'] = \
                    item.xpath(u"div[@class='htype_info clearfix']/div[@class='htype_info_pic left']/img[1]")[0].attrib[
                        'src']  # 首页图片url
                detail_pics = ''
                lis = item.xpath(
                    u"div[@class='htype_info_list btddd']/table[@class='htype-table']/tbody[1]/tr[@class='ht_tr_other']/td[@class='ht_other']/ul[@class='ht_pic_list clearfix']/li")
                if lis:
                    for li in lis:
                        detail_pics += li.xpath("img[1]")[0].attrib['bigimgurl'] + '##'
                room_infos['picture_list_url'] = detail_pics.rstrip('##')  # 房间图片列表url
                room_infos["max_occupancy"] = ''
                if item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[5]"):
                    if item.xpath(
                            u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[5]/span[@class='vm']"
                    ):
                        room_infos["max_occupancy"] = item.xpath(
                            u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[5]/span[@class='vm']"
                        )[0].text
                    else:
                        room_infos["max_occupancy"] = str(len(item.xpath(
                            u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[5]/i[@class='icon_live_per']"
                        )))  # 入住人数信息
                # print room_infos["max_occupancy"]
                if not item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[9]"):
                    room_infos["internet_service"] = ''
                else:
                    room_infos["internet_service"] = item.xpath(
                        u"div[@class='htype_info clearfix']/div[@class='htype_info_nt']/p[@class='htype_info_ty']/span[9]")[
                        0].text  # 宽带信息
                room_infos['has_window'] = ''
                room_infos['has_own_toilet'] = ''
                room_infos['has_public_toliet'] = ''
                room_infos['has_toiletries'] = ''
                room_infos['has_slippers'] = ''
                room_infos['has_hot_water'] = ''
                room_infos['has_air_conditioning'] = ''
                room_infos['has_fridge'] = ''
                room_infos['has_computer'] = ''
                room_infos['has_tv'] = ''
                room_infos['has_balcony'] = ''
                room_infos['has_kitchen'] = ''
                room_infos['has_bar'] = ''
                room_infos['has_free_ddd'] = ''
                room_infos['has_free_idd'] = ''
                room_infos['has_breakfast'] = ''
                room_infos['booking_website_number'] = ''
                room_infos['confirm_type'] = ''
                room_infos['status'] = '0'
                room_infos['remark'] = ''
                room_infos['crawl_version'] = ''
                room_infos['crawl_time'] = timestamp
                room_infos['reserved_col1'] = ''
                room_infos['reserved_col2'] = ''
                room_infos['reserved_col3'] = ''
                room_infos['reserved_col4'] = ''
                room_infos['reserved_col5'] = ''
                trs = item.xpath(
                    u"div[@class='htype_info_list btddd']/table[@class='htype-table']/tbody[1]/tr[@data-handle='rp']")
                for tr in trs:
                    # 产品字段
                    sub_room_infos = {}
                    sub_room_infos['channel_id'] = 3
                    sub_room_infos['platform_id'] = 1
                    sub_room_infos['product_id'] = tr.attrib['data-rpid'] + tr.attrib['data-sroomid'] + tr.attrib[
                        'data-rptype']  # 产品id
                    # print sub_room_infos['product_id']
                    sub_room_infos['product_name'] = tr.xpath(u"td[@class='ht_name']/span[1]")[0].text  # 产品名称
                    sub_room_infos['product_url'] = 'http://hotel.elong.com/%s/' % (hotel_id)  # 产品url
                    sub_room_infos['hotel_id'] = hotel_id
                    sub_room_infos['room_type_id'] = room_infos['room_type_id']
                    sub_room_infos['agent_name'] = tr.xpath(u"td[@class='ht_supply']")[0].text.replace("\n",
                                                                                                       "").replace("\r",
                                                                                                                   "")  # 供应商
                    sub_room_infos['agent_id'] = ''
                    pay_class = tr.xpath(u"td[@class='ht_pay']/i")[0].attrib['class']
                    if pay_class == 'icon_yufu':
                        sub_room_infos['product_type'] = u'预付'
                    elif pay_class == 'icon_danbao':
                        sub_room_infos['product_type'] = u'担保'
                    else:
                        sub_room_infos['product_type'] = ''
                    sub_room_infos['check_in_type'] = ''
                    sub_room_infos['is_series'] = ''
                    sub_room_infos['series_days'] = ''
                    # print(sub_room_infos['supply'])
                    sub_room_infos['breakfast_type'] = tr.xpath(u"td[@class='ht_brak']")[0].text  # 早餐
                    # print(sub_room_infos['breakfast'])
                    sub_room_infos['cancel_policy'] = tr.xpath(u"td[@class='ht_rule']/span[@class='ht_rule_free']")[
                        0].text  # 规则
                    sub_room_infos['confirm_type'] = ''  # 确认类型
                    if tr.xpath(u"td[@class='ht_name']/i[@class='icon_comit']"):
                        sub_room_infos['confirm_type'] = u'立即确认'
                    elif tr.xpath(u"td[@class='ht_name']/i[@class='icon_confirm']"):
                        sub_room_infos['confirm_type'] = tr.xpath(u"td[@class='ht_name']/i[@class='icon_confirm']")[
                            0].text
                    sub_room_infos['need_guarantee'] = ''
                    sub_room_infos['can_add_bed'] = ''
                    sub_room_infos['total_month_orders'] = ''
                    sub_room_infos['total_history_orders'] = ''
                    sub_room_infos['pay_type'] = ''
                    sub_room_infos['use_integral'] = ''
                    sub_room_infos['use_coupon'] = ''
                    sub_room_infos['gift_card'] = ''
                    if productsInfo[sub_index].has_key('giftDesc'):
                        for gift_index in range(len(productsInfo[sub_index]['giftDesc'])):
                            sub_room_infos['gift_card'] += productsInfo[sub_index]['giftDesc'][gift_index].replace('\r',
                                                                                                                   '').replace(
                                '\n', '') + '##'

                    sub_room_infos['gift_card'] = sub_room_infos['gift_card'].rstrip('##')  # 礼品卡
                    sub_room_infos['other_preferential_desc'] = ''
                    if tr.xpath(u"td[@class='ht_retu']/span[@method='coupon']"):
                        sub_room_infos['other_preferential_desc'] = \
                        tr.xpath(u"td[@class='ht_retu']/span[@method='coupon']")[0].text  # 其他优惠描述
                    sub_room_infos['crawl_version'] = ''
                    sub_room_infos['crawl_time'] = timestamp
                    sub_room_infos['reserved_col1'] = ''
                    sub_room_infos['reserved_col2'] = ''
                    sub_room_infos['reserved_col3'] = ''
                    sub_room_infos['reserved_col4'] = ''
                    sub_room_infos['reserved_col5'] = ''

                    # 价格字段
                    product_price = {}
                    product_price['channel_id'] = 3  # 渠道id：1、携程， 2、去哪儿，3、艺龙
                    product_price['hotel_id'] = hotel_id
                    product_price['platform_id'] = 1
                    product_price['room_type_id'] = room_infos['room_type_id']
                    product_price['product_id'] = sub_room_infos['product_id']
                    product_price['sell_date'] = check_in_date + ' 00:00:00'
                    product_price['price'] = str(
                        int(round(float(productsInfo[sub_index]['priceOfDays'][0]['salePrice']), 0)))  # 价格
                    product_price['preferential_price'] = str(
                        int(round(float(productsInfo[sub_index]['priceOfDays'][0]['price']), 0)))  # 优惠后价格
                    currency = productsInfo[sub_index]['priceOfDays'][0]['currency']
                    if currency == 'RMB':
                        product_price['currency'] = '1'
                    product_price['is_min_price'] = ''
                    product_price['can_book'] = None
                    if tr.xpath(u"td[@class='ht_book']/span"):
                        product_price['can_book'] = '0'
                    else:
                        product_price['can_book'] = '1'
                    product_price['stock'] = ''
                    product_price['stock_status'] = ''
                    product_price['reserved_col1'] = ''
                    product_price['reserved_col2'] = ''
                    product_price['reserved_col3'] = ''
                    product_price['reserved_col4'] = ''
                    product_price['reserved_col5'] = ''
                    product_price['store_status'] = ''
                    product_price['crawl_version'] = ''
                    product_price['crawl_time'] = timestamp
                    sub_room_infos['product_price'] = product_price
                    room_infos["sub_rooms"].append(sub_room_infos)
                    sub_index += 1
                if len(room_infos['sub_rooms']) is not 0:
                    results['room'].append(room_infos)

            # 记录原始数据
            original_date = {
                'id': response.meta['hotel_id'],
                'timestamp': timestamp,
                'data': body
            }
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_PRICE_RESOURSE_PATH')
            ycf_items['results'] = results
            yield ycf_items
            # results_str = json.dumps(results)
            # log.msg('elong hotel ' + 'data:' + results_str.replace('%', '%%'), level=log.INFO, spider='elong_sprider')
            # logger.info('elong hotel ' + 'data:' + results_str.replace('%', '%%'))
        except Exception, e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELPRICE
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def download_errback(self, e):
        print type(e), repr(e)

    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)