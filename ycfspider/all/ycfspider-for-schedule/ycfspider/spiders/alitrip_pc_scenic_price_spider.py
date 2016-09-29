# coding=utf-8
import sys

from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from ycfspider.items import YcfspiderItem
import re, os
import json, traceback
import copy
import datetime
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
import time
from ycfspider.utils.enum import *
from ycfspider.tables import *

class AlitripPcScenicPriceSpider(RedisSpider):
    name = 'AlitripPcScenicPriceSpider'
    start_urls = []
    # pc版请求头
    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:alitrip_pc_scenic_price_all'
    else:
        redis_key = 'spider:alitrip_pc_scenic_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'accept-encoding': 'gzip, deflate, sdch',
               'accept-language': 'zh-CN,zh;q=0.8',
               'cache-control': 'max-age=0',
               'cookie': 'cna=uRsWELyWbRUCAXkh0qIw2ry8; ck1=; uc3=sg2=BqPn64cCjA7wpIMYSklk5f20aEsE4eLTlRGpa6Uw38c%3D&nk2=saDbFuV5Cu%2FU8oW6pus%3D&id2=UUjRIpz4eBqRdw%3D%3D&vt3=F8dAS1HtopMvXRigtA4%3D&lg2=Vq8l%2BKCLz3%2F65A%3D%3D; lgc=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; hng=; _tb_token_=rhJNBIK8rz9N; uss=B0fyW6yKG4DtDVw%2BKY3cTYKafev5tnqV4vay7%2F6toHm26rGHcO6rc9Idio4%3D; tracknick=%5Cu4E00%5Cu8DEF%5Cu90FD%5Cu5728%5Cu5BFB%5Cu627E11; cookie2=1c5464647ee57d0cdb483dca8027aa03; skt=b667dc31a98c18b0; t=1139922cd4fa2e2096749748e098a161; tti=0ab26e8afefe0ab26e8acc96626972214t0cb1; l=Ap2do-SqHUE90jBJzPKezojALXOWcdEG; isg=AiAgnyqnY4lN-9-xIzJ4vlzw8S5ISgTzs1RbF5owJzvElca_QjurgtOnW4ru',
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"
               }

    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            url = 'https://s.alitrip.com/scenic/detail.htm?sid=' + str(item["scenic_id"])
            return Request(url, meta={'item': item}, headers=self.headers, dont_filter=True)

    def __init__(self, *args, **kwargs):
        super(AlitripPcScenicPriceSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH') + '/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH') + '/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
             raise DontCloseSpider

    # 解析景区详情页信息,获取产品套餐id
    def parse(self, response):
        try:
            data = response.meta.get("item")
            sid = data["scenic_id"]
            # list 存放产品
            list = []
            #渠道景区产品价格表
            scenic_product_price_table = {"id":"","channel_id":ChannelEnum.ALITRIP,"platform_id":PlatformEnum.PC,
            "scenic_id":sid,"scenic_ticket_class_id":"","scenic_ticket_type_id":"",
            "scenic_product_id":"","agent_id":"","sell_date":"","ticket_price":"","selling_price":"","currency":"",
            "preferential_desc":"","can_book":"","reserved_col1":"","reserved_col2":"",
            "reserved_col3":"","reserved_col4":"","reserved_col5":"","crawl_version":"","crawl_time":"",}

            #渠道景区产品表
            scenic_products_table = {"id":"","scenic_product_id":"","scenic_product_name":"","platform_id":PlatformEnum.PC,"channel_id":ChannelEnum.ALITRIP,
                "scenic_id":sid,"scenic_ticket_class_id":"","scenic_ticket_type_id":"","agent_id":"","agent_name":"","product_type":"",
                "support_digital":"","product_url":"","desc":"","valid_start_date":"","valid_end_date":"","book_policy":"","change_policy":"",
                "cancel_policy":"","admission_policy":"","fee_desc":"",
                "valid_time_desc":"","use_desc":"","ticket_get_time":"","ticket_get_type":"",
                "total_month_orders":"","total_history_orders":"","pay_type":"","pay_platform":"","use_integral":"","use_coupon":"",
                "has_invoice":"","sort":"","lastest_book_info":"","current_visit_info":"","reserved_col1":"",
                "reserved_col2":"","reserved_col3":"","reserved_col4":"","reserved_col5":"","crawl_version":"","crawl_time":time.strftime("%Y-%m-%d %H:%M:%S")}

            #渠道景区票券类型表
            scenic_ticket_types_table = {
                "id": "", "scenic_ticket_class_id": "", "scenic_ticket_class_name": "", "scenic_ticket_type_id": "",
                "scenic_ticket_type_name": "", "platform_id": PlatformEnum.PC, "channel_id": ChannelEnum.ALITRIP, "scenic_id": sid, "desc": "",
                "remark": "",
                "reserved_col1": "", "reserved_col2": "", "reserved_col3": "", "reserved_col4": "", "reserved_col5": "",
                "crawl_version": "", "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),"product_list":[]
            }

            sel = Selector(response)
            prid_xath = '//*[@ class="tickets-list-wrap J_TriggerNav"]/li'
            cspuid_list = sel.xpath(prid_xath).extract()
            if cspuid_list :
                for i in range(0,len(cspuid_list)):
                    #票型id、名称、价格、门票价格
                    cspuid_a_xpath = prid_xath+'['+str(i)+']'+ '/div/@data-cspuid'
                    name_a_xpath = prid_xath+'['+str(i)+']'+ '/div//*@[class="ticket-name"]/text()'
                    agent_price_a_xpath = prid_xath+'['+str(i)+']'+ '/div//*@[class="pi-price pi-price-sm"]/text()'
                    scenic_price_a_xpath = prid_xath+'['+str(i)+']'+ '/div//*@[class="pi-price pi-price-lg"]/text()'

                    cspuid_a = sel.xpath(cspuid_a_xpath).extract()
                    name_a = sel.xpath(name_a_xpath).extract()
                    agent_price_a = sel.xpath(agent_price_a_xpath).extract()
                    scenic_price_a = sel.xpath(scenic_price_a_xpath).extract()

                    if cspuid_a :
                        a_ticket = scenic_ticket_types_table.copy()
                        a_ticket["scenic_ticket_type_id"] = cspuid_a[0]
                        if name_a:
                            a_ticket["scenic_ticket_type_name"] = name_a[0]
                        #     if "电子票" in a_ticket["scenic_ticket_type_name"]:
                        #         a_ticket["product_type"] = 1
                        #     elif "实体票" in a_ticket["scenic_ticket_type_name"]:
                        #         a_ticket["product_type"] = 2
                        # if agent_price_a:
                        #     a_ticket["agent_price"] = agent_price_a[0]
                        # if scenic_price_a:
                        #     a_ticket["scenic_price"] = scenic_price_a[0]

                        url = "https://s.alitrip.com/scenic/more.htm?&callback=jsonp312&format=json&" \
                              "sid="+ str(sid) +"&cspuid="+str(a_ticket.get("scenic_ticket_type_id"))+"&moreseller=true&faceprice=--&jumpto=1&_input_charset=utf-8"
                        yield Request(url,meta={"scenic_ticket_type":a_ticket,"sid":sid,"list":list},callback=self.parse_product,dont_filter=True,headers=self.headers)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ALITRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    #获取产品页数、解析门票信息
    def parse_product(self,response):
        try:
            body = response.body
            res = body[10:len(body)-1]
            json_res = json.loads(res)
            results_ticket = response.meta["ticket"]

            sid = response.meta.get("sid")
            cspuid = results_ticket.get("scenic_ticket_type_id")
            #存放产品队列
            list = response.meta.get("list")
            result = json_res.get("result")
            if result:
                total = result.get("total")
                pageSize = result.get("pageSize")
                current = result.get("current")
                #总页数
                pageNum = total/pageSize if total % pageSize == 0 else total/pageSize + 1
                #解析当前数据、产品信息
                data = result.get("data")
                # 渠道景区产品表
                scenic_products_table = {"id": "", "scenic_product_id": "", "scenic_product_name": "",
                                         "platform_id": PlatformEnum.PC, "channel_id": ChannelEnum.ALITRIP,
                                         "scenic_id": sid, "scenic_ticket_class_id": "", "scenic_ticket_type_id": "",
                                         "agent_id": "", "agent_name": "", "product_type": "",
                                         "support_digital": "", "product_url": "", "desc": "", "valid_start_date": "",
                                         "valid_end_date": "", "book_policy": "", "change_policy": "",
                                         "cancel_policy": "", "admission_policy": "", "fee_desc": "",
                                         "valid_time_desc": "", "use_desc": "", "ticket_get_time": "", "ticket_get_type": "",
                                         "total_month_orders": "", "total_history_orders": "", "pay_type": "",
                                         "pay_platform": "", "use_integral": "", "use_coupon": "",
                                         "has_invoice": "", "sort": "", "lastest_book_info": "", "current_visit_info": "",
                                         "reserved_col1": "",
                                         "reserved_col2": "", "reserved_col3": "", "reserved_col4": "", "reserved_col5": "",
                                         "crawl_version": "", "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),"price_list":[]}
                if data:
                    products = data[0].get("tickets")
                    if "电子票" in results_ticket["scenic_ticket_type_name"]:
                        product_type = 1
                    elif "实体票" in results_ticket["scenic_ticket_type_name"]:
                        product_type= 2
                    for her_product in products:
                        my_product = scenic_products_table.copy()
                        #门票价格
                        # my_product[""] = her_product["faceprice"]
                        #收费项目id
                        my_product["scenic_product_id"] = her_product["itemid"]
                        my_product["scenic_ticket_type_id"] = cspuid
                        my_product["agent_id"] = her_product["sellerCode"]
                        my_product["agent_name"] = her_product["sellerName"]
                        my_product["product_type"] = product_type
                        my_product["support_digital"] = 1 if her_product["eticketStatus"] ==7 else 0
                        sku = her_product["skuPV"]
                        #票种id
                        skuid = her_product["skuid"]
                        my_product["product_url"] = 'https://items.alitrip.com/item.htm?id='+my_product["scenic_product_id"]
                        my_product["cancel_policy"] = her_product["refund"]
                        my_product["change_policy"] = her_product["modify"]
                        url = 'https://items.alitrip.com/item.htm?id=' + my_product[
                            "scenic_product_id"] + '&spm=181.7395991.1998089960.2.kNeKBg&sku=;' + str(
                            sku)
                        list.append(my_product)
                        #产品详情
                        yield Request(url, meta ={"result_scenic_products":my_product,"results_ticket":results_ticket,"skuid":skuid,"list":list},headers=self.headers,callback=self.parse_product_detail,dont_filter=True)

                #当前页数少于总页数、则继续发送请求。
                if current < pageNum :
                    url = "https://s.alitrip.com/scenic/more.htm?&callback=jsonp312&format=json&" \
                      "sid="+ str(sid) +"&cspuid="+str(cspuid)+"&moreseller=true&faceprice=--&jumpto="+str(current+1)+"&_input_charset=utf-8"
                    yield Request(url,meta={"results_ticket":results_ticket,"sid":sid,"list":list},callback=self.parse_product,dont_filter=True,headers=self.headers)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ALITRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    #解析产品详情、价格
    def parse_product_detail(self,response):
        try:
            result_scenic_products = response.meta.get("result_scenic_products")
            results_ticket = response.meta.get("results_ticket")
            skuid = response.meta.get("skuid")

            #取产品id,赋值价格队列
            product_list = response.meta.get("list")
            sel = Selector(response)
            scenic_product_id = result_scenic_products.get("scenic_product_id")
            # 渠道景区产品价格表
            scenic_product_price_table = {"id": "", "channel_id": ChannelEnum.ALITRIP, "platform_id": PlatformEnum.PC,
                                          "scenic_id": results_ticket.get("scenic_id"), "scenic_ticket_class_id": "", "scenic_ticket_type_id": result_scenic_products.get("scenic_ticket_type_id"),
                                          "scenic_product_id":scenic_product_id , "agent_id": result_scenic_products.get("agent_id"), "sell_date": "", "ticket_price": "",
                                          "selling_price": "", "currency": "",
                                          "preferential_desc": "", "can_book": "", "reserved_col1": "", "reserved_col2": "",
                                          "reserved_col3": "", "reserved_col4": "", "reserved_col5": "",
                                          "crawl_version": "", "crawl_time": time.strftime("%Y-%M-%d"),}
            # 市面价格
            ticket_price_xpath = '//*[@id="J_OriginPriceWrap"]/i'
            ticket_price = sel.xpath(ticket_price_xpath).extract()
            if ticket_price:
                ticket_price1 = ticket_price[0]
            price_xpath = '//*[@id="J_SKUJson"]'
            price_info = sel.xpath(price_xpath).extract()
            if price_info:
                price = price_info[0][16:len(price_info[0])-1]
                json_price = json.loads(price)
                price_list = []
                i = 0
                for (d,x) in json_price.items():
                    if x.get("skuId") == skuid and i < 7:
                        i += 1
                        scenic_product_price = scenic_product_price_table.copy()
                        scenic_product_price["selling_price"] = float(x.get("price"))/100
                        scenic_product_price["sell_date"] = x.get("date")
                        scenic_product_price["ticket_price"] = ticket_price1
                        #库存
                        # scenic_product_price["stock"] = x.get("stock")
                        #价格放入房型
                        price_list.append(scenic_product_price)
                        for sssssssss in product_list:
                            if scenic_product_id == sssssssss.get("scenic_product_id"):
                                sssssssss["price_list"]  = price_list

            #检查价格是否填满、填满后就yeild item
            flag = 0
            for ssssssss in product_list:
                if ssssssss["price_list"]:
                    flag = 1
                    break

            #产品放入票型
            if flag == 0:
                results_ticket["product_list"] = product_list
                ycf_items = YcfspiderItem()
                ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                    'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                ycf_items['results'] = results_ticket
                print ycf_items
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ALITRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))








































