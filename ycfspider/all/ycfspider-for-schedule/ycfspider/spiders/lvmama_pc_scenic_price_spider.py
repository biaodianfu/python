# coding=utf-8
import sys
import traceback
import os

reload(sys)
sys.setdefaultencoding("utf-8")
import json, datetime, time
from scrapy.conf import settings
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from  ycfspider.items import YcfspiderItem
import re
from scrapy.http import Request
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.redisutils import RedisUtil
import random
from ycfspider.utils.useragent import user_agent_list
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.enum import *
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord


class LvmamaPcScenicPriceSpider(RedisSpider):
    name = "LvmamaPcScenicPriceSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["http://s.lvmama.com/"]
    start_urls = []
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:lvmama_pc_scenic_price_all'
    else:
        redis_key = 'spider:lvmama_pc_scenic_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    headers = {
        "Accept": "image/webp,image/*,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "zh-CN,zh;q=0.8",
        "Connection": "keep-alive",
        "Host": "ticket.lvmama.com",
        "User-Agent": random.choice(user_agent_list)
    }

    def __init__(self, *args, **kwargs):
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(LvmamaPcScenicPriceSpider, self).__init__(*args, **kwargs)
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
                url = "http://ticket.lvmama.com/scenic-" + item["scenic_id"]
                return Request(url, meta={"item": item}, headers=self.headers, dont_filter=True)

    def parse(self, response):
        try:
            sel = Selector(response)
            isexist = sel.xpath('//*[@class="overview"]').extract()
            if len(isexist) > 0:
                data = response.meta["item"]
                item = {}
                item["scenic_id"] = data["scenic_id"]
                item["scenic_name"] = data["scenic_name"]
                item["city_name_cn"] = data["city_name"]
                item["channel_id"] = 7
                item['platform_id'] = 1
                ticket_xpath = '//*[@class="dpro-list"]/table[1]/tbody[@class="ptbox"]'
                ticket_list = sel.xpath(ticket_xpath).extract()
                scenic_tickets_info = []
                crawl_time = time.strftime("%Y-%m-%d %H:%M:%S")
                if ticket_list:
                    if len(ticket_list) > 0:
                        for i in range(len(ticket_list)):
                            tickets = {'scenic_ticket_class_id': '', 'scenic_ticket_class_name': '',
                                       "scenic_ticket_type_name": "", "scenic_ticket_type_id": "", "channel_id": 7,
                                       "scenic_id": "", "desc": "", "remark": "", "crawl_time": crawl_time,
                                       "crawl_version": "", "ticket_products": []}
                            tickets["scenic_id"] = data["scenic_id"]
                            tickets['platform_id'] = 1
                            tickets['reserved_col1'] = ''
                            tickets['reserved_col2'] = ''
                            tickets['reserved_col3'] = ''
                            tickets['reserved_col4'] = ''
                            tickets['reserved_col5'] = ''
                            # tickets["crawl_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                            ticket_type_xpath = ticket_xpath + '[' + str(i + 1) + ']/tr/td/div/h5/text()'
                            ticket_type = sel.xpath(ticket_type_xpath).extract()
                            if ticket_type:
                                tickets["scenic_ticket_type_name"] = ticket_type[0]
                            ticket_id_xpath = ticket_xpath + '[' + str(i + 1) + ']/@id'
                            ticket_id = sel.xpath(ticket_id_xpath).extract()
                            if ticket_id:
                                tickets["scenic_ticket_type_id"] = ticket_id[0]
                            product_list_xpath = ticket_xpath + '[' + str(i + 1) + ']/tr[1]/td[2]/div[1]/div[1]/dl'
                            product_list = sel.xpath(product_list_xpath).extract()
                            product = []
                            if product_list:
                                if len(product_list) > 0:
                                    for j in range(len(product_list)):
                                        good = {"scenic_product_name": "", "channel_id": 7, "scenic_id": "",
                                                'scenic_ticket_class_id': '', "scenic_ticket_type_id": "", "agent_name": "",
                                                "product_type": "", "product_url": "", "desc": "", "valid_start_date": "",
                                                "valid_end_date": "",
                                                "book_policy": "", "change_policy": "", "cancel_policy": "",
                                                "admission_policy": "", "fee_desc": "", "valid_time_desc": "",
                                                "use_desc": "", "ticket_get_time": "", "ticket_get_type": "",
                                                "total_month_orders": "",
                                                "total_history_orders": "", "pay_type": "", "pay_platform": "",
                                                "use_integral": "", "use_coupon": "", "has_invoice": "",
                                                "crawl_version": "", "crawl_time": crawl_time, "scenic_product_id": "",
                                                "product_price": []}
                                        good["scenic_id"] = data["scenic_id"]
                                        good["scenic_ticket_type_id"] = tickets["scenic_ticket_type_id"]
                                        good["agent_name"] = "lvmama"
                                        good['platform_id'] = 1
                                        good['agent_id'] = ''
                                        good['reserved_col1'] = ''
                                        good['reserved_col2'] = ''
                                        good['reserved_col3'] = ''
                                        good['reserved_col4'] = ''
                                        good['reserved_col5'] = ''
                                        good["product_type"] = None
                                        # good["crawl_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                                        product_price = {"channel_id": "", "platform_id": "", "scenic_id": "",
                                                         'scenic_ticket_class_id': '', "scenic_ticket_type_id": "",
                                                         "scenic_product_id": "", "sell_date": "", "ticket_price": "",
                                                         "selling_price": "", "currency": "", "can_book": "",
                                                         "crawl_version": "", "crawl_time": crawl_time,
                                                         "preferential_desc": ""}
                                        product_price['reserved_col1'] = ''
                                        product_price['reserved_col2'] = ''
                                        product_price['reserved_col3'] = ''
                                        product_price['reserved_col4'] = ''
                                        product_price['reserved_col5'] = ''
                                        product_price["channel_id"] = 7
                                        product_price["scenic_id"] = data["scenic_id"]
                                        product_price["scenic_ticket_type_id"] = tickets["scenic_ticket_type_name"]
                                        product_price["currency"] = 1
                                        product_price["platform_id"] = 1
                                        product_price["sell_date"] = crawl_time
                                        book_policy__xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dt[@class="pdname"]/span[1]'
                                        book_policy = sel.xpath(book_policy__xpath).extract()
                                        if book_policy:
                                            good["book_policy"] = sel.xpath(book_policy__xpath + '/em/text()').extract()[
                                                                      0] + "##" + \
                                                                  sel.xpath(book_policy__xpath + '/i/text()').extract()[0]
                                        pay_type_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dd[@class="pdpaytype"]/span[1]/i[1]/text()'
                                        pay_type = sel.xpath(pay_type_xpath).extract()
                                        if pay_type:
                                            good["pay_type"] = pay_type[0].replace("\r", "").replace("\n", "").replace(" ",
                                                                                                                       "")

                                        can_book_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dd[@class="pdpaytype"]/span[1]/a[1]/text()'
                                        can_book = sel.xpath(can_book_xpath).extract()
                                        if can_book:
                                            product_price["can_book"] = 1

                                        product_url_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dt[@class="pdname"]/a/@href'
                                        product_url = sel.xpath(product_url_xpath).extract()
                                        if product_url:
                                            if 'javascript' in product_url[0]:
                                                good["product_url"] = ''
                                            else:
                                                good["product_url"] = product_url[0]
                                                product_id = re.findall(r"[0-9]+", product_url[0])
                                                good["scenic_product_id"] = product_id[0]
                                                product_price["scenic_product_id"] = product_id[0]
                                        product_name_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dt[@class="pdname"]/a/@title'
                                        product_name = sel.xpath(product_name_xpath).extract()
                                        if product_name:
                                            good["scenic_product_name"] = product_name[0]
                                        product_id_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dt[@class="pdname"]/a/@data'
                                        product_id = sel.xpath(product_id_xpath).extract()
                                        if product_id:
                                            good["scenic_product_id"] = product_id[0]
                                            product_price["scenic_product_id"] = product_id[0]
                                        product_orig_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dd[@class="pdprice"]/del/text()'
                                        product_orig = sel.xpath(product_orig_xpath).extract()
                                        if product_orig:
                                            # good["orig_price"] = product_orig[0].replace("\n","").replace("\t","")
                                            product_price["ticket_price"] = product_orig[0].replace("\n", "").replace("\t",
                                                                                                                      "").replace(
                                                "\r", "").replace(" ", "")
                                        product_actu_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dd[@class="pdlvprice"]/dfn/i/text()'
                                        product_actu = sel.xpath(product_actu_xpath).extract()
                                        if product_actu:
                                            # good["actu_price"] = product_actu[0]
                                            product_price["selling_price"] = product_actu[0]
                                        product_discount_xpath = product_list_xpath + '[' + str(
                                            j + 1) + ']/dd[@class="pdprefer"]/span'
                                        product_discount = sel.xpath(product_discount_xpath).extract()
                                        product_discount_list = ''
                                        if len(product_discount) > 0:
                                            for k in range(len(product_discount)):
                                                product_discount_list_xpath = product_discount_xpath + '[' + str(
                                                    k + 1) + ']/'
                                                discount = ''
                                                if sel.xpath(product_discount_list_xpath + "@tip-content").extract():
                                                    discount = discount + sel.xpath(
                                                        product_discount_list_xpath + "@tip-content").extract()[0]
                                                if sel.xpath(product_discount_list_xpath + "em/text()").extract():
                                                    discount = discount + "|||" + sel.xpath(
                                                        product_discount_list_xpath + "em/text()").extract()[0] + "|||" + \
                                                               sel.xpath(
                                                                   product_discount_list_xpath + "i/text()").extract()[0]
                                                if sel.xpath(product_discount_list_xpath + "text()").extract():
                                                    discount = discount + "|||" + \
                                                               sel.xpath(product_discount_list_xpath + "text()").extract()[
                                                                   0]
                                                if len(product_discount_list) > 0:
                                                    product_discount_list = product_discount_list + "##" + discount
                                                else:
                                                    product_discount_list = discount
                                        product_price["preferential_desc"] = product_discount_list.replace("\r",
                                                                                                           "").replace("\n",
                                                                                                                       "").replace(
                                            " ", "")
                                        good["product_price"] = product_price
                                        product.append(good)
                            tickets["ticket_products"] = product
                            scenic_tickets_info.append(tickets)
                item["tickets"] = scenic_tickets_info
                if len(item["tickets"]) > 0:
                    results = {}
                    results['tickets'] = item['tickets']
                    results['scenic_id'] = item['scenic_id']
                    results['scenic_name'] = item['scenic_name']
                    results['city_name_cn'] = item['city_name_cn']
                    results['channel_id'] = item['channel_id']
                    results['platform_id'] = item['platform_id']
                    # item['results'] = results

                    # 记录原始数据
                    original_date = {
                        'id': item['scenic_id'],
                        'timestamp': crawl_time,
                        'data': response.body
                    }
                    self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
                    # ycf_items = YcfspiderItem()
                    # ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                    # ycf_items['results'] = results
                    # yield ycf_items
                    # yield item


                    tickets = item["tickets"]
                    # tickets_type = ["成人票", "双人票", "亲子票", "儿童票", "团体票", "相关票", "老人票", "学生票", "家庭票", "特殊群体票", "特惠套餐票", "特卖会",
                    #                 "其它票", "特殊人群票"]
                    # ticket_type_plus = ["交通+酒店", "跟团游", '门票+酒店']
                    product_num = 0
                    for i in range(len(tickets)):
                        product_num = product_num + len(tickets[i]["ticket_products"])
                    for i in range(len(tickets)):
                        for j in range(len(tickets[i]["ticket_products"])):
                            scenic_product_id = tickets[i]["ticket_products"][j]["scenic_product_id"]
                            if scenic_product_id:
                                tickets_type = 1
                                product_url = tickets[i]["ticket_products"][j]["product_url"]
                                url = "http://www.lvmama.com/vst_front/book/ticket/getTicketTimePrice.do?suppGoodsId=" + scenic_product_id + "&visitTime=" + (
                                datetime.datetime.now() + datetime.timedelta(days=1)).strftime(
                                    '%Y-%m-%d') + "&distributorId="
                                if product_url:
                                    if 'dujia' in product_url:
                                        date = datetime.datetime.now().strftime('%Y%m')
                                        endDate = (datetime.datetime.now() + datetime.timedelta(days=30)).strftime('%Y%m')
                                        tickets_type = 2
                                        url = 'http://dujia.lvmama.com/group/data.json?productId=%s&date=%s&endDate=%s&monthType=1' % (scenic_product_id, date, endDate)
                                meta = {"item": item, "tickets_type": tickets_type,
                                        "product_num": product_num, "product_id": scenic_product_id,
                                        "scenic_id": item["scenic_id"]}
                                yield Request(url, meta=meta, dont_filter=True, headers=self.headers,
                                              callback=self.parse_date_price)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.LVMAMA
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_date_price(self, response):
        try:
            crawl_time = time.strftime("%Y-%m-%d %H:%M:%S")
            # 记录原始数据
            original_date = {
                'id': response.meta['scenic_id'],
                'timestamp': crawl_time,
                'data': response.body
            }
            try:
                self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            except:
                print traceback.format_exc()
            scenic_id = response.meta["scenic_id"]
            resp_item = response.meta["item"]
            product_num = response.meta["product_num"]
            tickets_type = response.meta["tickets_type"]
            product_id = response.meta["product_id"]
            # tickets_type_list = ["成人票", "双人票", "亲子票", "儿童票", "团体票", "相关票", "老人票", "学生票", "家庭票", "特殊群体票", "特惠套餐票", "特卖会",
            #                         "其它票", "特殊人群票"]
            # ticket_type_plus_list = ["交通+酒店", "跟团游", '门票+酒店']
            redis_count_key_pre = 'scenic_price_count:7_'
            redis_cache_key_pre = 'scenic_price_cache:7_'
            try:
                data = json.loads(response.body)
                if tickets_type == 1:
                    for price in data["attributes"]["timePriceList"]:
                        key = '%s-%s' % (product_id, price['specDateStr'])
                        self.r.hset(redis_cache_key_pre + str(scenic_id), key, json.dumps(price))
                elif tickets_type == 2:
                    for price in data:
                        key = '%s-%s' % (product_id, price['date'])
                        self.r.hset(redis_cache_key_pre + str(scenic_id), key, json.dumps(price))
            except:
                print traceback.format_exc()
            if product_num == self.r.incr(redis_count_key_pre + str(scenic_id)):
                for i in range(30):  # 之后30天数据
                    day_time = (datetime.datetime.now() + datetime.timedelta(days=i + 1)).strftime("%Y-%m-%d")
                    for ticket in resp_item["tickets"]:
                        for product in ticket['ticket_products']:
                            price_str = self.r.hget(redis_cache_key_pre + str(scenic_id),
                                                        product['scenic_product_id'] + '-' + day_time)
                            if price_str:
                                price = json.loads(price_str)
                                product_url = product['product_url']
                                if product_url:
                                    if 'dujia' in product_url:
                                        #度假频道
                                        if 'price' in price:
                                            product['product_price']['selling_price'] = price['price']
                                    else:
                                        # 团购、特卖会
                                        if 'marketPrice' in price:
                                            product['product_price']['ticket_price'] = price['markerPriceYuan']
                                            product['product_price']['selling_price'] = price['priceYuan']
                                else:
                                    if 'marketPrice' in price:
                                        #门票类
                                        product['product_price']['ticket_price'] = price['markerPriceYuan']
                                        product['product_price']['selling_price'] = price['priceYuan']
                            else:
                                product['product_price']['can_book'] = 0
                            product['product_price']['sell_date'] = day_time + ' 00:00:00'
                    results = {}
                    results['tickets'] = resp_item['tickets']
                    results['scenic_id'] = resp_item['scenic_id']
                    results['scenic_name'] = resp_item['scenic_name']
                    results['city_name_cn'] = resp_item['city_name_cn']
                    results['channel_id'] = resp_item['channel_id']
                    results['platform_id'] = resp_item['platform_id']
                    # resp_item['results'] = results
                    ycf_items = YcfspiderItem()
                    ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                        'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                    ycf_items['results'] = results
                    yield ycf_items
                    # 解析完成后从redis删除数据
                self.r.delete(redis_cache_key_pre + str(scenic_id))
                self.r.delete(redis_count_key_pre + str(scenic_id))
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.LVMAMA
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta["product_id"]
            error_log_dic['pid'] = response.meta["item"]['scenic_id']
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)