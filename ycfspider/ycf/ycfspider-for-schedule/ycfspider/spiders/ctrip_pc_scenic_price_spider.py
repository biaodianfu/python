# coding=utf-8
import sys
import traceback

from ycfspider.utils.enum import *

reload(sys)
sys.setdefaultencoding("utf-8")

import requests
import datetime
import types
from scrapy.conf import settings
from ycfspider.items import YcfspiderItem
import json
import os
from lxml import etree
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
import re
from scrapy.http import FormRequest, Request
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.redisutils import RedisUtil
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord


class CtripPcScenicPriceSpider(RedisSpider):
    name = "CtripPcScenicPriceSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["piao.ctrip.com"]
    start_urls = []
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:ctrip_pc_scenic_price_all'
    else:
        redis_key = 'spider:ctrip_pc_scenic_price_ycf'
    # settings = get_project_settings()

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    day = 30

    def __init__(self, *args, **kwargs):
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(CtripPcScenicPriceSpider, self).__init__(*args, **kwargs)
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
                item = eval(item)
                req = FormRequest(url=item["url"], dont_filter=True,
                                  meta={'scenic_id': item["scenic_id"], 'scenic_name': item['scenic_name']})
                self.crawler.engine.crawl(req, spider=self)

    def parse(self, response):
        try:
            item = {}
            scenic_id = response.meta['scenic_id']
            item["scenic_id"] = response.meta['scenic_id']
            item['platform_id'] = 1
            if type(response.meta['scenic_name']) is not types.UnicodeType:
                item['scenic_name'] = response.meta['scenic_name'].decode('unicode-escape')
            else:
                item['scenic_name'] = response.meta['scenic_name']
            # print item['scenic_name']
            # time.sleep(0.5)
            content = response.body

            contentTree = etree.HTML(content, etree.HTMLParser(encoding="utf-8"))
            item['channel_id'] = 1

            pattern1 = re.compile("cityName:'(.*?)',\s", re.S)
            city = re.findall(pattern1, content)
            if len(city):
                city = city[0]
            else:
                city = ''
            # print city
            item['city_name_cn'] = city
            crawl_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            i = 0
            ticket_item_list = []
            ticket_items = []
            ticket_item = {}
            list_product_id = []
            price_info = contentTree.xpath("//div[@id='J-Ticket']/table[@class='ticket-table']/tbody/tr")
            for p in price_info:
                # time.sleep(0.5)
                # print p.xpath("string(.)")
                if (i % 2 == 0):
                    ticket_type = p.xpath(".//td[@class='ticket-type']/span")
                    if (len(ticket_type) == 1):
                        if (len(ticket_item) != 0):
                            ticket_item['ticket_products'] = ticket_item_list
                            ticket_items.append(ticket_item)
                            ticket_item = {}
                        current_ticket_type = ticket_type[0].text
                        # print current_ticket_type
                        ticket_item['scenic_ticket_type_name'] = current_ticket_type
                        ticket_item['scenic_ticket_type_id'] = ''
                        ticket_item['channel_id'] = 1
                        ticket_item['scenic_id'] = response.meta['scenic_id']
                        ticket_item['desc'] = ''
                        ticket_item['platform_id'] = 1
                        ticket_item['scenic_ticket_class_id'] = ''
                        ticket_item['scenic_ticket_class_name'] = ''
                        ticket_item['remark'] = ''
                        ticket_item['crawl_version'] = ''
                        ticket_item['crawl_time'] = crawl_time
                        ticket_item['reserved_col1'] = ''
                        ticket_item['reserved_col2'] = ''
                        ticket_item['reserved_col3'] = ''
                        ticket_item['reserved_col4'] = ''
                        ticket_item['reserved_col5'] = ''

                    ticket_list = {}
                    ticket_list['channel_id'] = 1
                    scenic_product_id = p.xpath(".//@data-id")
                    if len(scenic_product_id):
                        ticket_list['scenic_product_id'] = scenic_product_id[0]
                    else:
                        ticket_list['scenic_product_id'] = ''
                    list_product_id.append(ticket_list['scenic_product_id'])
                    ticket_title_type = p.xpath(".//td[@class='ticket-title-wrapper']/span")
                    if (len(ticket_title_type) > 0):
                        ticket_title_type = ticket_title_type[0].text
                    else:
                        ticket_title_type = ''

                    ticket_name = p.xpath(".//td[@class='ticket-title-wrapper']/a")
                    if (len(ticket_name) > 0):
                        ticket_name = ticket_name[0].text
                    else:
                        ticket_name = ''
                    ticket_list['scenic_product_name'] = ticket_title_type + "##" + ticket_name
                    # print ticket_list['scenic_product_name']

                    ticket_list['scenic_id'] = response.meta['scenic_id']

                    ticket_list['scenic_ticket_type_id'] = ''
                    ticket_list['agent_name'] = ''
                    ticket_list['agent_id'] = ''
                    ticket_info_url = 'http://piao.ctrip.com/Thingstodo-Booking-ShoppingWebSite/api/TicketStatute?resourceID=' + \
                                      ticket_list['scenic_product_id']
                    desc = ''
                    try:
                        proxies = {
                            "https": response.meta['proxy']
                        }
                        ticket_info = requests.get(url=ticket_info_url, proxies=proxies, timeout=5)  # 最基本的GET请求
                        ticket_info.encoding = 'utf-8'
                        ticket_info = ticket_info.content
                        ticket_info = json.loads(ticket_info)

                        cost_include = person_type = exchange_method = important_items = refund_rule = receipt = ''
                        if 'data' in ticket_info:
                            data = ticket_info['data']
                            for d in data:
                                if d['title'] == u'费用包含':
                                    cost_include = d['desc']
                                    if cost_include == None:
                                        cost_include = ''
                                    desc = desc + u'费用包含|||' + cost_include.replace('\n', "").replace(' ', '') + u"##"
                                elif d['title'] == u'适用人群':
                                    person_type = d['desc']
                                    if person_type == None:
                                        person_type = ''
                                    desc = desc + u'适用人群|||' + person_type.replace('\n', "").replace(' ', '') + u"##"
                                elif d['title'] == u'重要条款':
                                    important_items = d['desc']
                                    if important_items == None:
                                        important_items = ''
                                    desc = desc + u"重要说明|||" + important_items.replace('\n', "").replace(' ', '') + u"##"
                                elif d['title'] == u'兑换方式':
                                    exchange_method = d['desc']
                                    if exchange_method == None:
                                        exchange_method = ''
                                    desc = desc + u"兑换方式|||" + exchange_method.replace('\n', "").replace(' ', '') + u"##"
                                elif d['title'] == u'退改规则':
                                    refund_rule = d['desc']
                                    if refund_rule == None:
                                        refund_rule = ''
                                    desc = desc + u"退改规则|||" + refund_rule.replace('\n', "").replace(' ', '') + u"##"
                                elif d['title'] == u'发票说明':
                                    receipt = d['desc']
                                    if receipt == None:
                                        receipt = ''
                                    desc = desc + u"发票说明|||" + receipt.replace('\n', "").replace(' ', '')
                    except:
                        error_log_dic = {}
                        error_log_dic['channel_id'] = ChannelEnum.CTRIP
                        error_log_dic['platform_id'] = PlatformEnum.PC
                        error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
                        error_log_dic['id'] = response.meta['scenic_id']
                        error_log_dic['pid'] = ''
                        error_log_dic['error_info'] = traceback.format_exc()
                        error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
                        error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.logger_error.error(json.dumps(error_log_dic))
                    # print desc
                    ticket_list['desc'] = desc
                    ticket_list['change_policy'] = refund_rule
                    ticket_list['cancel_policy'] = refund_rule
                    ticket_list['admission_policy'] = important_items
                    ticket_list['fee_desc'] = cost_include
                    ticket_list['valid_time_desc'] = ''
                    ticket_list['used_desc'] = ''
                    # print exchange_method
                    if exchange_method.find(u"取票时间") == -1:
                        ticket_list['ticket_get_time'] = ''
                    else:
                        exchange = exchange_method.split(u"取票时间")[1]
                        if exchange[0] == u'：':
                            exchange = exchange[1:]
                        if exchange.find(u"<br/>") == -1:
                            ticket_list['ticket_get_time'] = ''
                        else:
                            ticket_list['ticket_get_time'] = exchange.split(u"<br/>")[0]
                            # print ticket_list['ticket_get_time']
                    if exchange_method.find(u"补充说明") == -1:
                        ticket_list['ticket_get_type'] = ''
                    else:
                        ticket_list['ticket_get_type'] = exchange_method.split(u"补充说明：")

                    ticket_list['total_month_orders'] = ''
                    ticket_list['total_history_orders'] = ''

                    if exchange_method.find(u'凭携程数字验证码') == -1:
                        ticket_list['support_digit'] = 0
                    else:
                        ticket_list['support_digit'] = 1

                    if receipt.find(u"如需发票") != -1:
                        ticket_list['has_invoice'] = 1
                    else:
                        ticket_list['has_invoice'] = 0

                    ticket_list['total_month_orders'] = ''
                    ticket_list['total_history_orders'] = ''

                    ticket_list['product_url'] = ''
                    ticket_list['valid_start_date'] = ''
                    ticket_list['valid_end_date'] = ''

                    ticket_list['product_type'] = 1

                    if (len(ticket_type) == 1):
                        ticket_preorder_time = p.xpath(".//td[3]")
                        ticket_pay_ways = p.xpath(".//td[7]")
                    else:
                        ticket_preorder_time = p.xpath(".//td[2]")
                        ticket_pay_ways = p.xpath(".//td[6]")

                    if (len(ticket_preorder_time) > 0):
                        ticket_list['book_policy'] = ticket_preorder_time[0].text.strip()
                    else:
                        ticket_list['book_policy'] = ''
                    # print ticket_list['book_policy']

                    if len(ticket_pay_ways) > 0:
                        ticket_list['pay_type'] = ticket_pay_ways[0].text.strip()
                    else:
                        ticket_list['pay_type'] = ''
                    # print ticket_list['pay_type']
                    ticket_list['pay_platform'] = ''
                    ticket_list['use_integral'] = ''
                    ticket_list['use_coupon'] = ''

                    ticket_list['crawl_version'] = ''
                    ticket_list['crawl_time'] = crawl_time
                    ticket_list['reserved_col1'] = ''
                    ticket_list['reserved_col2'] = ''
                    ticket_list['reserved_col3'] = ''
                    ticket_list['reserved_col4'] = ''
                    ticket_list['reserved_col5'] = ''
                    ticket_list['reserved_col5'] = ''
                    product_price = {}
                    # product_price['id'] = None
                    product_price['channel_id'] = 1
                    product_price['platform_id'] = 1
                    product_price['scenic_id'] = response.meta['scenic_id']
                    product_price['scenic_ticket_type_id'] = ''
                    # product_price['scenic_ticket_class_id'] = ''
                    product_price['scenic_product_id'] = ticket_list['scenic_product_id']
                    product_price['sell_date'] = crawl_time
                    ticket_market_price = p.xpath(".//td[@class='del-price']/strong")
                    if (len(ticket_market_price) > 0):
                        product_price['ticket_price'] = ticket_market_price[0].text.strip()
                    else:
                        product_price['ticket_price'] = ''
                    # print product_price['ticket_price']

                    ticket_ctrip_price = p.xpath(".//td/span[@class='ctrip-price']/strong")
                    if (len(ticket_ctrip_price) > 0):
                        product_price['selling_price'] = ticket_ctrip_price[0].text.strip()
                    else:
                        product_price['selling_price'] = ''
                    # print product_price['selling_price']
                    if (len(product_price['selling_price'])):
                        product_price['can_book'] = 1
                    else:
                        product_price['can_book'] = 0
                    # print product_price['can_book']

                    product_price['currency'] = 1

                    ticket_benifit = ''
                    benifit = p.xpath(".//td[@class='icon-wrapper pop-wrapper']/span[@class='icon-back']")
                    if (len(benifit) > 0):
                        ticket_benifit = benifit[0].xpath("string(.)").replace(' ', '').replace('\n', '').strip()
                    benifit = p.xpath(".//td[@class='icon-wrapper pop-wrapper']/span[@class='icon-mobile has-word']")
                    if (len(benifit) > 0):
                        if len(ticket_benifit) > 0:
                            ticket_benifit = ticket_benifit + "##手机端" + benifit[0].xpath("string(.)").replace(' ',
                                                                                                              '').replace(
                                '\n', '').stip()
                        else:
                            ticket_benifit = benifit[0].xpath("string(.)").replace(' ', '').replace('\n', '').strip()
                    product_price['preferential_desc'] = ticket_benifit
                    # print product_price['preferential_desc']
                    product_price['crawl_version'] = ''
                    product_price['crawl_time'] = crawl_time
                    product_price['reserved_col1'] = ''
                    product_price['reserved_col2'] = ''
                    product_price['reserved_col3'] = ''
                    product_price['reserved_col4'] = ''
                    product_price['reserved_col5'] = ''
                    ticket_list['product_price'] = product_price
                    ticket_item_list.append(ticket_list)
                i = i + 1
            if len(ticket_item_list):
                ticket_item['ticket_products'] = ticket_item_list
            ticket_items.append(ticket_item)
            item['tickets'] = ticket_items
            # items = YcfspiderItem()
            # 记录原始数据
            original_date = {
                'id': item['scenic_id'],
                'timestamp': crawl_time,
                'data': response.body
            }
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            # items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_PRICE_RESOURSE_PATH')
            # items['results'] = item
            # yield items
            for product_id in list_product_id:
                url = 'http://piao.ctrip.com/Thingstodo-Booking-BookingWebSite/api/TicketOrderInputApi/action/GetDatePriceInfo?resourceID=%s' % product_id
                meta = {'productId': product_id, 'sightId': scenic_id, 'len': len(list_product_id), 'results': item}
                yield Request(url=url, dont_filter=True, callback=self.parse_price, meta=meta)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_price(self, response):
        try:
            sightId = response.meta['sightId']
            productId = response.meta['productId']
            redis_count_key_pre = 'scenic_price_count:1_'
            redis_cache_key_pre = 'scenic_price_cache:1_'
            original_date = {
                'id': sightId,
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': response.body
            }
            try:
                self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
                data = json.loads(response.body)
                for price in data['data']:
                    key = '%s-%s' % (productId, price['date'])
                    self.r.hset(redis_cache_key_pre + str(sightId), key, json.dumps(price))
            except:
                error_log_dic = {}
                error_log_dic['channel_id'] = ChannelEnum.CTRIP
                error_log_dic['platform_id'] = PlatformEnum.PC
                error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
                error_log_dic['id'] = response.meta['productId']
                error_log_dic['pid'] = response.meta['sightId']
                error_log_dic['error_info'] = traceback.format_exc()
                error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
                error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger_error.error(json.dumps(error_log_dic))
            len = response.meta['len']

            if len == self.r.incr(redis_count_key_pre + str(sightId)):
                result = response.meta['results']
                checkInDate = datetime.datetime.now()
                for i in range(0, self.day):
                    date_str = checkInDate.strftime('%Y-%m-%d')
                    for ticket in result['tickets']:
                        for product in ticket['ticket_products']:
                            price_str = self.r.hget(redis_cache_key_pre + str(sightId),
                                                    product['scenic_product_id'] + '-' + date_str)
                            if price_str:
                                price = json.loads(price_str)
                                if 'price' in price:
                                    product['product_price']['selling_price'] = price['price']
                                    # 团购（可用）
                                    # else:
                                    #     scenic_product_name = product['scenic_product_name']
                                    #     product['scenic_product_name'] = scenic_product_name + '##团购'
                            else:
                                product['product_price']['can_book'] = 0
                            product['product_price']['sell_date'] = date_str + ' 00:00:00'
                    ycf_items = YcfspiderItem()
                    ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                        'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                    ycf_items['results'] = result
                    delta = datetime.timedelta(days=1)
                    checkInDate = checkInDate + delta
                    # 解析完成后从redis删除数据
                    # print ycf_items
                    yield ycf_items
                self.r.delete(redis_count_key_pre + str(sightId))
                self.r.delete(redis_cache_key_pre + str(sightId))
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta['productId']
            error_log_dic['pid'] = response.meta['sightId']
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)