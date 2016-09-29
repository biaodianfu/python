# coding=utf-8
import sys
import traceback
import os

reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy.conf import settings
from scrapy.http import Request
from scrapy.exceptions import DontCloseSpider
import json
import random
from ycfspider.items import YcfspiderItem
from scrapy_redis.spiders import RedisSpider
from scrapy.http import FormRequest
import datetime
import re
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.useragent import user_agent_list
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.enum import *
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord


class QunarPcScenicPriceSpider(RedisSpider):
    name = 'QunarPcScenicPriceSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    custom_settings = {
        'RETRY_TIMES': 5
    }
    if not os.path.exists(filename):
            os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:qunar_pc_scenic_price_all'
    else:
        redis_key = 'spider:qunar_pc_scenic_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    header = {'Host': 'piao.qunar.com',
              'Accept': 'application/json, text/javascript, */*; q=0.01',
              'Origin:': 'http://piao.qunar.com',
              'X-Requested-With': 'XMLHttpRequest',
              'User-Agent': random.choice(user_agent_list),
              'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'Accept-Encoding': 'gzip, deflate',
              'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6'
              }
    day = 30

    def __init__(self, *args, **kwargs):
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(QunarPcScenicPriceSpider, self).__init__(*args, **kwargs)
        self.update_settings(settings)
        if settings.get('SCALE') == 'all':
            self.r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            self.r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
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
                j = json.loads(item)
                url = 'http://piao.qunar.com/ticket/detail/getTickets.json'
                data = {'sightId': str(j['id']), 'from': 'detail'}
                referer = 'http://piao.qunar.com/ticket/detail_' + str(j['id']) + '.html'
                self.header['Referer'] = referer
                req = FormRequest(url=url, formdata=data, dont_filter=True, meta=j, headers=self.header)
                self.crawler.engine.crawl(req, spider=self)

    def parse(self, response):
        try:
            data = json.loads(response.body)
            results = {}
            if len(data['data']['groups']) > 0:
                sightId = response.meta['id']
                scenic_name = response.meta['name']
                results['channel_id'] = 2
                results['platform_id'] = 1
                results['scenic_id'] = sightId
                results['scenic_name'] = scenic_name
                results['city_name_cn'] = response.meta['city']
                results['tickets'] = []
                crawl_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                list_product_id = []
                for group in data['data']['groups']:
                    for products in group:
                        scenic_ticket_type = {}
                        scenic_ticket_type['scenic_ticket_type_id'] = products['typeId']
                        scenic_ticket_type['scenic_ticket_type_name'] = products['typeName']
                        scenic_ticket_type['channel_id'] = 2
                        scenic_ticket_type['scenic_id'] = sightId
                        scenic_ticket_type['platform_id'] = 1
                        scenic_ticket_type['desc'] = ''
                        scenic_ticket_type['scenic_ticket_class_name'] = products['ticketZoneName']
                        scenic_ticket_type['scenic_ticket_class_id'] = ''
                        scenic_ticket_type['remark'] = ''
                        scenic_ticket_type['crawl_version'] = ''
                        scenic_ticket_type['crawl_time'] = crawl_time
                        scenic_ticket_type['reserved_col1'] = ''
                        scenic_ticket_type['reserved_col2'] = ''
                        scenic_ticket_type['reserved_col3'] = ''
                        scenic_ticket_type['reserved_col4'] = ''
                        scenic_ticket_type['reserved_col5'] = ''
                        scenic_ticket_type['ticket_products'] = []
                        for product in products['tickets']:
                            scenic_product = {}
                            scenic_product['scenic_product_id'] = product['productId']
                            list_product_id.append(product['productId'])
                            scenic_product['scenic_product_name'] = product['title']
                            scenic_product['scenic_ticket_class_id'] = ''
                            scenic_product['channel_id'] = 2
                            scenic_product['platform_id'] = 1
                            scenic_product['agent_id'] = ''
                            scenic_product['scenic_id'] = sightId
                            scenic_product['scenic_ticket_type_id'] = products['typeId']
                            scenic_product['agent_name'] = product['supplierName']
                            scenic_product['product_type'] = ''
                            scenic_product['support_digital'] = ''
                            scenic_product['product_url'] = 'http://piao.qunar.com/ticket/detail_%s.html' % sightId
                            scenic_product['desc'] = ''
                            bookAtAnyTimeStr = product['bookAtAnyTimeStr']
                            if bookAtAnyTimeStr == u'可订明日':
                                checkInDate = datetime.datetime.now()
                                delta = datetime.timedelta(days=1)
                                checkOutDate = checkInDate + delta
                                scenic_product['valid_start_date'] = checkOutDate.strftime('%Y-%m-%d') + ' 00:00:00'
                            elif bookAtAnyTimeStr == u'可订今日':
                                checkInDate = datetime.datetime.now()
                                scenic_product['valid_start_date'] = checkInDate.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                reg = u'可订([0-9]{1,2})月([0-9]{1,2})日'
                                matcher = re.match(reg, bookAtAnyTimeStr)
                                if matcher:
                                    m = matcher.group(1)
                                    if len(m) < 2:
                                        m = '0' + m
                                    d = matcher.group(2)
                                    if len(d) < 2:
                                        d = '0' + d
                                    year = datetime.datetime.now().year
                                    scenic_product['valid_start_date'] = '%s-%s-%s 00:00:00' % (year, m, d)
                            scenic_product['valid_end_date'] = ''
                            scenic_product['book_policy'] = bookAtAnyTimeStr
                            scenic_product['change_policy'] = ''
                            scenic_product['cancel_policy'] = product['refundDescription']
                            scenic_product['admission_policy'] = ''
                            scenic_product['fee_desc'] = ''
                            scenic_product['valid_time_desc'] = ''
                            scenic_product['use_desc'] = ''
                            scenic_product['ticket_get_time'] = ''
                            scenic_product['ticket_get_type'] = ''
                            scenic_product['total_month_orders'] = ''
                            scenic_product['total_history_orders'] = ''
                            scenic_product['pay_type'] = ''
                            scenic_product['pay_platform'] = ''
                            scenic_product['use_integral'] = ''
                            scenic_product['use_coupon'] = ''
                            scenic_product['has_invoice'] = ''
                            scenic_product['crawl_version'] = ''
                            scenic_product['crawl_time'] = crawl_time
                            scenic_product['reserved_col1'] = ''
                            scenic_product['reserved_col2'] = ''
                            scenic_product['reserved_col3'] = ''
                            scenic_product['reserved_col4'] = ''
                            scenic_product['reserved_col5'] = ''
                            product_price = {}
                            product_price['channel_id'] = 2
                            product_price['platform_id'] = 1
                            product_price['scenic_id'] = sightId
                            product_price['scenic_ticket_type_id'] = products['typeId']
                            product_price['scenic_ticket_class_id'] = ''
                            product_price['scenic_product_id'] = product['productId']
                            product_price['sell_date'] = crawl_time
                            product_price['ticket_price'] = product['marketPrice']
                            product_price['selling_price'] = product['qunarPrice']
                            product_price['currency'] = 1
                            if product['cashBack'] > 0:
                                product_price['preferential_desc'] = u'红包最高返%s元' % product['cashBack']
                            else:
                                product_price['preferential_desc'] = ''
                            product_price['can_book'] = 1
                            product_price['crawl_version'] = ''
                            product_price['crawl_time'] = crawl_time
                            product_price['reserved_col1'] = ''
                            product_price['reserved_col2'] = ''
                            product_price['reserved_col3'] = ''
                            product_price['reserved_col4'] = ''
                            product_price['reserved_col5'] = ''
                            scenic_product['product_price'] = product_price
                            scenic_ticket_type['ticket_products'].append(scenic_product)
                        results['tickets'].append(scenic_ticket_type)
                # 记录原始数据
                original_date = {
                    'id': sightId,
                    'timestamp': crawl_time,
                    'data': response.body
                }
                self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
                # ycf_items = YcfspiderItem()
                # ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                # ycf_items['results'] = results
                # yield ycf_items
                # item['id'] = sightId
                # item['name'] = scenic_name
                # item['city'] = response.meta['city']
                # yield item
                for product_id in list_product_id:
                    url = 'http://piao.qunar.com/web/priceCalendar.json?method=queryProductDate&productId=%s' % product_id
                    meta = {'productId': product_id, 'sightId': sightId, 'len': len(list_product_id),
                            'results': results}
                    yield Request(url=url, dont_filter=True, callback=self.parse_price, meta=meta, headers=self.header)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta['id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_price(self, response):
        redis_count_key_pre = 'scenic_price_count:2_'
        redis_cache_key_pre = 'scenic_price_cache:2_'
        sightId = response.meta['sightId']
        try:
            # 记录原始数据
            crawl_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            original_date = {
                'id': sightId,
                'timestamp': crawl_time,
                'data': response.body
            }
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            productId = response.meta['productId']
            len = response.meta['len']
            data = json.loads(response.body)
            for price in data['data']:
                key = '%s-%s' % (productId, price['useDate'])
                self.r.hset(redis_cache_key_pre + str(sightId), key, json.dumps(price))
        except:
            self.log_error_scenic_id(response, ErrorTypeEnum.PARSEERROR)
        try:
            cache_count = self.r.incr(redis_count_key_pre + str(sightId))
        except:
            self.log_error_scenic_id(response, ErrorTypeEnum.LOGICERROR)
        if len == cache_count:
            result = response.meta['results']
            checkInDate = datetime.datetime.now()
            for i in range(0, self.day):
                try:
                    date_str = checkInDate.strftime('%Y-%m-%d')
                    for ticket in result['tickets']:
                        for product in ticket['ticket_products']:
                            price_str = self.r.hget(redis_cache_key_pre + str(sightId),
                                                    product['scenic_product_id'] + '-' + date_str)
                            if price_str:
                                price = json.loads(price_str)
                                if 'marketPrice' in price:
                                    product['product_price']['ticket_price'] = price['marketPrice']
                                    product['product_price']['selling_price'] = price['qunarPrice']
                                # 团购（可用）
                                else:
                                    scenic_product_name = product['scenic_product_name']
                                    product['scenic_product_name'] = scenic_product_name + '##团购'
                            else:
                                product['product_price']['can_book'] = 0
                            product['product_price']['sell_date'] = date_str + ' 00:00:00'

                    delta = datetime.timedelta(days=1)
                    checkInDate = checkInDate + delta
                    ycf_items = YcfspiderItem()
                    ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                        'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                    ycf_items['results'] = result
                    yield ycf_items
                except:
                    self.log_error_scenic_id(response, ErrorTypeEnum.PARSEERROR)
            # 解析完成后从redis删除数据
            self.r.delete(redis_cache_key_pre + str(sightId))
            self.r.delete(redis_count_key_pre + str(sightId))

    def log_error_scenic_id(self, response, error_type):
        error_log_dic = {}
        error_log_dic['channel_id'] = ChannelEnum.QUNAR
        error_log_dic['platform_id'] = PlatformEnum.PC
        error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
        error_log_dic['id'] = response.meta['productId']
        error_log_dic['pid'] = response.meta['sightId']
        error_log_dic['error_info'] = traceback.format_exc()
        error_log_dic['error_type'] = error_type
        error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.logger_error.error(json.dumps(error_log_dic))

    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)