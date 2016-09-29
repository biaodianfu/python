# encoding=utf-8
import copy
import json
import random
import traceback
import datetime
import sys
import os

from scrapy_redis.spiders import RedisSpider

from scrapy.http import Request
from scrapy.conf import settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.items import YcfspiderItem
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import *
from ycfspider.utils.useragent import user_agent_list


# from ycfspider.utils.spider_state_flag_record import spiderStateRecord

__author__ = 'lizhipeng'

reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
    'Host': 'lvyou.meituan.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    # 'userid': 63298865,
    'User-Agent': 'AiMeiTuan /HONOR-5.1.1-KIW-TL00H-1776x1080-480-7.0.1-401-860707030258106-huawei1'
}


class MeituanAppScenicPriceSpider(RedisSpider):
    name = 'MeituanAppScenicPriceSpider'
    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:meituan_app_scenic_price_all'
    else:
        redis_key = 'spider:meituan_app_scenic_price_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(MeituanAppScenicPriceSpider, self).__init__(*args, **kwargs)
        if settings.get('SCALE') == 'all':
            self.r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            self.r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        self.day = 30
        self.logger_data = Logger(settings.get('LOG_PATH') + '/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH') + '/' + self.name + '/error/')
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
            # city_id = item['city_id']
            ticket_id = item['ticket_id']
            ticket_name = item['ticket_name']
            city_name = item['city_name_cn']
            # ticket_id = 625435
            taget_url = 'http://lvyou.meituan.com/volga/api/v5/trip/deal/poi/full/{ticket_id}?onsale=1&mpt_poiid={ticket_id}&mypos=' \
                        '23.124211%2C113.373884&fields=id%2Cchannel%2Cimgurl%2Csquareimgurl%2Ctitle%2Ctitle5%2Ctit' \
                        'le6%2Cprice%2Cvalue%2Cbrandname%2Crating%2Crate-count%2Csolds%2Cstatus%2Ccate%2Csubcate%2C' \
                        'start%2Cend%2Cnobooking%2CattrJson%2Ccampaigns%2Cterms%2Cshowtype%2Coptionalattrs%2Crdploc%2' \
                        'Crdcount%2Cbookingphone%2CtripTicketType%2CtripProductType%2CdealTagIds%2Ccanbuyprice%2C' \
                        'campaignprice%2Ccampaigntag%2CnewSoldsString&client=android&utm_source=huawei1&utm_medium=' \
                        'android&utm_term=401&version_name=7.0.1&utm_content=860707030258106&utm_campaign=AgroupBgroup' \
                        'C0D200E1369226328121443072_c1_e12245678122244676555_a%25e9%2597%25a8%25e7%25a5%25a8%2B%25e6%25' \
                        'b0%25b4%25e4%25b8%258a%25e4%25b9%2590%25e5%259b%25ad_b100002Ghomepage_search&ci=20&msid=8607' \
                        '070302581061468805343739&userid=63298865&__reqTraceID=8afe225f-3649-4fac-87a5-cd425c46c04d&__skck=6a375bce8c66a0dc2' \
                        '93860dfa83833ef&__skts=1468805393010&__skua=1b65af47f716a751dca998365104f554&__skno=c3a95383' \
                        '-0d27-40b1-8695-bfa8e3d72e89&__skcy=0didccnmMCfk2QP2zzKb236zGd4%3D'.format(ticket_id=ticket_id)
            meta = {
                # 'city_id': city_id,
                'ticket_id': str(ticket_id),
                'retry_count': 0,
                'taget_url': taget_url,
                'ticket_name': ticket_name,
                'city_name': city_name
            }

            req = Request(url=taget_url, meta=meta, headers=headers, callback=self.parse_ticket_info, dont_filter=True,
                          errback=self.download_errback)
            self.crawler.engine.crawl(req, spider=self)

    def parse_ticket_info(self, response):
        try:
            ticket_item = {}
            body = response.body
            # logger_data.info('meituan ticket original data:' + body)
            ticket_json = json.loads(body)
            meta = response.meta
            results = {
                'scenic_id': meta['ticket_id'],
                'tickets': [],
                'scenic_name': meta['ticket_name'],
                'channel_id': 4,
                'platform_id': 2,
                'city_name_cn': meta['city_name']
            }
            ticket_datas = ticket_json['data']
            results['tickets'] = tickets = []
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            meta['timestamp'] = timestamp
            product_id_list = []
            package_id_list = []
            for ticket_type in ticket_datas:
                stids = ticket_type['stids']
                if len(stids):
                    stid = stids[0]['stid'].split('_')[1]
                    scenic_ticket_class_id = stid[0:7]
                product_name = ticket_type.get('productName', '')
                product_type = ticket_type.get('prductType', '')
                scenic_ticket_class_name = (product_name + '##' + product_type).rstrip('##')
                if ticket_type['needTicketGroup']:
                    for ticket_sub_type in ticket_type['productModels']:
                        scenic_ticket_type = {}
                        scenic_ticket_type['channel_id'] = 4
                        scenic_ticket_type['scenic_ticket_class_id'] = scenic_ticket_class_id
                        scenic_ticket_type['scenic_ticket_class_name'] = scenic_ticket_class_name
                        scenic_ticket_type['scenic_ticket_type_id'] = ''
                        scenic_ticket_type['scenic_ticket_type_name'] = ''
                        scenic_ticket_type['scenic_id'] = meta['ticket_id']
                        scenic_ticket_type['desc'] = ''
                        scenic_ticket_type['remark'] = stids[0]['stid']
                        scenic_ticket_type['crawl_version'] = ''
                        scenic_ticket_type['crawl_time'] = timestamp
                        scenic_ticket_type['ticket_products'] = []
                        scenic_ticket_type['scenic_ticket_type_name'] = ticket_sub_type.get('ticketName', '')
                        scenic_ticket_type['platform_id'] = 2
                        scenic_ticket_type['reserved_col1'] = ''
                        scenic_ticket_type['reserved_col2'] = ''
                        scenic_ticket_type['reserved_col3'] = ''
                        scenic_ticket_type['reserved_col4'] = ''
                        scenic_ticket_type['reserved_col5'] = ''
                        for ticket_product in ticket_sub_type['ticketDeals']:
                            product = {}
                            self.get_product(ticket_product, product, scenic_ticket_type, stids, meta, product_id_list,
                                             package_id_list, 1)
                            scenic_ticket_type['ticket_products'].append(product)
                        tickets.append(scenic_ticket_type)
                else:
                    scenic_ticket_type = {}
                    scenic_ticket_type['channel_id'] = 4
                    scenic_ticket_type['scenic_ticket_class_id'] = scenic_ticket_class_id
                    scenic_ticket_type['scenic_ticket_class_name'] = scenic_ticket_class_name
                    scenic_ticket_type['scenic_ticket_type_id'] = ''
                    scenic_ticket_type['scenic_ticket_type_name'] = ''
                    scenic_ticket_type['scenic_id'] = meta['ticket_id']
                    scenic_ticket_type['desc'] = ''
                    scenic_ticket_type['remark'] = stids[0]['stid']
                    scenic_ticket_type['crawl_version'] = ''
                    scenic_ticket_type['crawl_time'] = timestamp
                    scenic_ticket_type['ticket_products'] = []
                    scenic_ticket_type['platform_id'] = 2
                    scenic_ticket_type['reserved_col1'] = ''
                    scenic_ticket_type['reserved_col2'] = ''
                    scenic_ticket_type['reserved_col3'] = ''
                    scenic_ticket_type['reserved_col4'] = ''
                    scenic_ticket_type['reserved_col5'] = ''
                    for ticket_sub_type in ticket_type['productModels']:
                        product = {}
                        self.get_product(ticket_sub_type, product, scenic_ticket_type, stids, meta, product_id_list,
                                         package_id_list, 2)
                        scenic_ticket_type['ticket_products'].append(product)
                    tickets.append(scenic_ticket_type)
            ticket_item['results'] = results
            ticket_item['ticket_id'] = meta['ticket_id']
            ticket_item['ticket_name'] = meta['ticket_name']
            # 记录原始数据
            original_date = {
                'id': ticket_item['ticket_id'],
                'timestamp': timestamp,
                'data': response.body
            }
            self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            if len(package_id_list):
                for package in package_id_list:
                    meta = {
                        'package': package,
                        'type': 3,
                        'ticket_item': ticket_item,
                        'list_len': len(package_id_list),
                        'product_id_list': product_id_list
                    }
                    package_url = 'http://dabao.meituan.com/trippackage/api/deal/{package_id}/info/v1?fields=channel%2' \
                                  'Cktvplan%2Cmealcount%2Cdeposit%2Ctag%2Cterms%2ChotelExt%2Csolds%2Cnewrating%2Cdtype' \
                                  '%2Cvalue%2Crate-count%2Cimgurl%2Cpricecalendar%2Coptionalattrs%2Cstatus%2Cmenu%2C' \
                                  'bookinginfo%2Ccampaigns%2Cfakerefund%2Cannouncementtitle%2Cprice%2Cstart%2Csatisfac' \
                                  'tion%2Cslug%2Crecreason%2Csecurityinfo%2Ccate%2Cvoice%2Crange%2Ctodayavaliable%2Csq' \
                                  'uareimgurl%2Cmlls%2Crdploc%2Cid%2Ctitle%2Crefund%2Ccoupontitle%2Cmurl%2Cend%2Ccampai' \
                                  'gnprice%2Cmname%2Crdcount%2Cbrandname%2Cctype%2Cshowtype%2Csubcate%2Csevenrefund%2Ca' \
                                  'ttrJson%2Chowuse%2Crating%2Cnobooking%2Cisappointonline%2Ccanbuyprice%2Cbookingphone%' \
                                  '2Ccurcityrdcount%2Cexpireautorefund%2Cstate&mpt_dealid=38694206&utm_source=huawei1&u' \
                                  'tm_medium=android&utm_term=411&version_name=7.1.1&utm_content=860707030258106&utm_cam' \
                                  'paign=AgroupBgroupC0D100E2089803971251486208_c2_v8964145038896953345__2_e134276730549' \
                                  '54192101_c3_a%25e9%2597%25a8%25e7%25a5%25a8_b100002_t16704164962366700087&ci=20&msid' \
                                  '=8607070302581061469175945082&uuid=C0B4E41F9AFBA21BAFBB8E973A76412377A8777EEF994B4D79' \
                                  'EE62CC70A674B2&userid=63298865&__reqTraceID=8ea99a8f-a2ea-417c-92d7-c73f99c4f550&__' \
                                  'skck=6a375bce8c66a0dc293860dfa83833ef&__skts=1469178604433&__skua=9578b51c2abf1eb9b93' \
                                  '81f6c7350ef50&__skno=a769ae03-86b6-4b1c-9f6a-63389f767275&__skcy=5ITV8dLwyf1CJdjyC6O' \
                                  'bpeA%2BEO0%3D'.format(package_id=package['package_id'])
                    req = Request(url=package_url, meta=meta, headers=headers, callback=self.parse_next_level,
                                  dont_filter=True, errback=self.download_errback)
                    self.crawler.engine.crawl(req, spider=self)
            elif len(product_id_list):
                for product in product_id_list:
                    if product['type'] == 1:
                        product_url = 'http://i.meituan.com/lvyou/meilv/trade/ticket/api/price_stock/query/v1?feclient=' \
                                      'lvyou_wap&source=mt&client=wap&dealId={product_id}&source=mt&client=wap'.format(
                            product_id=product['product_id'])
                    elif product['type'] == 2:
                        product_url = 'http://i.meituan.com/lvyou/meilv/trade/api/price_stock/v1/{product_id}?feclient=' \
                                      'lvyou_wap&source=mt&client=wap&source=mt&client=m'.format(
                            product_id=product['product_id'])
                    meta = {
                        'product': product,
                        'type': product['type'],
                        'ticket_item': ticket_item,
                        'list_len': len(product_id_list),
                    }
                    headers['User-Agent'] = random.choice(user_agent_list)
                    req = Request(url=product_url, meta=meta, headers=headers, callback=self.parse_next_level,
                                  dont_filter=True, errback=self.download_errback)
                    self.crawler.engine.crawl(req, spider=self)


                    # yield ticket_item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = response.meta['ticket_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
            # logger_error.error(traceback.format_exc())
            # logger_error.info('meituan error:' + 'retry '+str(response.meta['retry_count'])+' times fail '+'ticket_name:'
            #             + response.meta['ticket_name']+' ticket_id:' + response.meta['ticket_id'])

    def get_product(self, ticket_product, product, scenic_ticket_type, stids, meta, product_id_list, package_id_list,
                    class_type):
        product['channel_id'] = 4
        product_dic = {}
        package_dic = {}
        product['platform_id'] = 2
        product['scenic_product_id'] = str(ticket_product['id'])
        product['scenic_product_name'] = ticket_product['title']
        product['scenic_id'] = scenic_ticket_type['scenic_id']
        product['scenic_ticket_class_id'] = scenic_ticket_type['scenic_ticket_class_id']
        if class_type == 1:
            if scenic_ticket_type['scenic_ticket_type_id'] == '':
                for stid in stids:
                    if stid['dealid'] == ticket_product['id']:
                        scenic_ticket_type['scenic_ticket_type_id'] = stid['stid'].split('_')[1][1:10]
            product['scenic_ticket_type_id'] = scenic_ticket_type['scenic_ticket_type_id']
        else:
            product['scenic_ticket_type_id'] = ''
        product['agent_name'] = ''
        product['agent_id'] = ''
        product['reserved_col1'] = ''
        product['reserved_col2'] = ''
        product['reserved_col3'] = ''
        product['reserved_col4'] = ''
        product['reserved_col5'] = ''
        if 'provider' in ticket_product:
            product['agent_name'] = ticket_product['provider'].get('title', '')
        product['product_type'] = ''
        product['support_digital'] = ''
        product['product_url'] = 'http://i.meituan.com/deal/%s.html' % (product['scenic_product_id'])
        product['desc'] = ''
        product['valid_start_date'] = ''
        product['valid_end_date'] = ''
        product['book_policy'] = ''
        product['change_policy'] = ''
        product['cancel_policy'] = ''
        product['admission_policy'] = ''
        product['fee_desc'] = ''
        product['valid_time_desc'] = ''
        product['use_desc'] = ''
        desc_list = []
        if 'terms' in ticket_product:
            for term in ticket_product['terms']:
                desc_list.append(term)
        if 'buynotes' in ticket_product:
            for note in ticket_product['buynotes']:
                desc_list.append(note)
        for desc in desc_list:
            content = ''
            try:
                for per_content in desc['content']:
                    if desc.get('type', '') == 'nest':
                        for sub_per_content in per_content['content']:
                            content += sub_per_content + ' '
                    else:
                        content += per_content + ''
            except Exception, e:
                print traceback.format_exc()
            content = content.rstrip(' ')
            if desc['title'] == u'有效期':
                product['valid_start_date'] = content
                product['valid_end_date'] = content
                product['valid_time_desc'] = content
            if desc['title'] == u'预订说明':
                product['book_policy'] = content
            if desc['title'] == u'退款说明':
                product['cancel_policy'] = content
            if desc['title'] == u'费用包含':
                product['fee_desc'] = content
            if desc['title'] == u'使用方法':
                product['use_desc'] = content
            product['desc'] += desc.get('title', '') + '|||' + content + '##'
        product['desc'] = product['desc'].rstrip('##').replace('"', '')
        product['ticket_get_time'] = ''
        product['ticket_get_type'] = ''
        product['total_month_orders'] = str(ticket_product.get('monthSolds', ''))
        product['total_history_orders'] = ticket_product.get('newSoldsString', '')
        product['pay_type'] = ''
        product['pay_platform'] = ''
        product['use_integral'] = ''
        product['use_coupon'] = ''
        product['has_invoice'] = ''
        product['crawl_version'] = ''
        product['crawl_time'] = meta['timestamp']

        product_price = {}
        product_price['channel_id'] = 4
        product_price['platform_id'] = '2'
        product_price['scenic_id'] = scenic_ticket_type['scenic_id']
        product_price['scenic_ticket_type_id'] = product['scenic_ticket_type_id']
        product_price['scenic_ticket_class_id'] = product['scenic_ticket_class_id']
        product_price['scenic_product_id'] = product['scenic_product_id']
        product_price['sell_date'] = meta['timestamp']
        product_price['ticket_price'] = str(ticket_product.get('value', ''))
        product_price['selling_price'] = str(ticket_product.get('canbuyprice', ''))
        product_price['currency'] = '1'
        product_price['preferential_desc'] = ''
        if 'campaigns' in ticket_product:
            for campaign in ticket_product['campaigns']:
                product_price['preferential_desc'] += campaign.get('logo', '') + ':' + campaign.get('longtitle',
                                                                                                    '') + '##'
        product_price['preferential_desc'] = product_price['preferential_desc'].rstrip('##')
        product_price['can_book'] = ''
        product_price['crawl_version'] = ''
        product_price['crawl_time'] = meta['timestamp']
        product_price['reserved_col1'] = ''
        product_price['reserved_col2'] = ''
        product_price['reserved_col3'] = ''
        product_price['reserved_col4'] = ''
        product_price['reserved_col5'] = ''
        product['product_price'] = product_price

        # TC为酒店+景区，需要进一步解析套餐
        if ticket_product['tripProductType'].get('value', '') == 'TC':
            package_dic['package_id'] = ticket_product['id']
            package_dic['scenic_ticket_class_id'] = scenic_ticket_type['scenic_ticket_class_id']
            package_dic['type'] = 3
            package_dic['scenic_id'] = scenic_ticket_type['scenic_id']
            package_id_list.append(package_dic)
        elif ticket_product['tripProductType'].get('value', '') == 'MP':
            product_dic['scenic_id'] = scenic_ticket_type['scenic_id']
            product_dic['product_id'] = ticket_product['id']
            product_dic['type'] = 1
            product_id_list.append(product_dic)
        else:
            product_dic['scenic_id'] = scenic_ticket_type['scenic_id']
            product_dic['product_id'] = ticket_product['id']
            product_dic['type'] = 2
            product_id_list.append(product_dic)

    def parse_next_level(self, response):
        try:
            # 记录原始数据
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            original_date = {
                'id': response.meta['ticket_item']['ticket_id'],
                'timestamp': timestamp,
                'data': response.body
            }
            try:
                self.logger_data.info(json.dumps(original_date).replace('%', '%%'))
            except:
                print traceback.format_exc()
            meta = response.meta
            body = response.body
            data = json.loads(body)
            redis_count_key_pre = 'scenic_price_count:4_'
            redis_cache_key_pre = 'scenic_price_cache:4_'
            if meta['type'] == 3:
                scenic_id = meta['package']['scenic_id']
                package_id = meta['package']['package_id']
                package_info = []
                self.get_package_info(data, package_info, scenic_id, package_id)
                key = 'P-%s' % (package_id)
                self.r.hset(redis_cache_key_pre + str(scenic_id), key, json.dumps(package_info))
            elif meta['type'] == 1:
                scenic_id = meta['product']['scenic_id']
                product_id = meta['product']['product_id']
                self.get_mp_price(data, product_id, scenic_id)
            elif meta['type'] == 2:
                scenic_id = meta['product']['scenic_id']
                product_id = meta['product']['product_id']
                self.get_lv_price(data, product_id, scenic_id)
            elif meta['type'] == 4:
                scenic_id = meta['product']['scenic_id']
                product_id = meta['product']['product_id']
                self.get_tc_price(data, product_id, scenic_id)
            # else:
            #     product_id = meta['product']['product_id']
            #     key = '%s-%s' % (product_id, price['useDate'])
            #     self.r.hset(sightId, key, json.dumps(price))
            list_len = meta['list_len']
            if list_len == self.r.incr(redis_count_key_pre + str(scenic_id)):
                if meta['type'] == 3:
                    self.get_package(meta, redis_cache_key_pre)
                    self.r.delete(redis_count_key_pre + str(scenic_id))
                    self.r.delete(redis_cache_key_pre + str(scenic_id))
                    # ycf_items = YcfspiderItem()
                    # ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                    # ycf_items['results'] = meta['ticket_item']['results']
                    # yield ycf_items
                    product_id_list = meta['product_id_list']
                    ticket_item = meta['ticket_item']
                    for product in product_id_list:
                        if product['type'] == 1:
                            product_url = 'http://i.meituan.com/lvyou/meilv/trade/ticket/api/price_stock/query/v1?feclient=' \
                                          'lvyou_wap&source=mt&client=wap&dealId={product_id}&source=mt&client=wap'.format(
                                product_id=product['product_id'])
                        elif product['type'] == 2:
                            product_url = 'http://i.meituan.com/lvyou/meilv/trade/api/price_stock/v1/{product_id}?feclient=' \
                                          'lvyou_wap&source=mt&client=wap&source=mt&client=m'.format(
                                product_id=product['product_id'])
                        elif product['type'] == 4:
                            product_url = 'http://dabao.meituan.com/trippackage/api/order/priceCalendar/{product_id}/v1?utm_source' \
                                          '=huawei1&utm_medium=android&utm_term=411&version_name=7.1.1&utm_content=860707030' \
                                          '258106&utm_campaign=AgroupBgroupC111866389250304_d032001003001_g6_f61734D100E20' \
                                          '89803971251486208_c2_v1423023418319134322__2_e10575743573382083397_c2_a%25e9%2' \
                                          '597%25a8%25e7%25a5%25a8_b100003Ghomepage_search&ci=20&msid=860707030258106146' \
                                          '9168034593&uuid=C0B4E41F9AFBA21BAFBB8E973A76412377A8777EEF994B4D79EE62CC70A674B2&' \
                                          'userid=63298865&__reqTraceID=4138769d-ff52-46e4-b8d9-d95c137e473e&__skck=6a375bc' \
                                          'e8c66a0dc293860dfa83833ef&__skts=1469169377321&__skua=9578b51c2abf1eb9b9381f6c735' \
                                          '0ef50&__skno=ba2440e6-5b22-45e7-9fed-3d338c5102cf&__skcy=C4dnwNLoOGFcM8VYjXv5FnbtHQQ%3D'.format(
                                product_id=product['product_id'])
                        meta = {
                            'product': product,
                            'type': product['type'],
                            'ticket_item': ticket_item,
                            'list_len': len(product_id_list),
                        }
                        headers['User-Agent'] = random.choice(user_agent_list)
                        req = Request(url=product_url, meta=meta, headers=headers, callback=self.parse_next_level,
                                      dont_filter=True, errback=self.download_errback)
                        self.crawler.engine.crawl(req, spider=self)
                        # yield meta['ticket_item']
                else:
                    result = meta['ticket_item'].get('results')
                    scenic_id = meta['product']['scenic_id']
                    checkInDate = datetime.datetime.now()
                    for i in range(0, self.day):
                        date_str = checkInDate.strftime('%Y-%m-%d')
                        for ticket in result['tickets']:
                            for product in ticket['ticket_products']:
                                price_str = self.r.hget(redis_cache_key_pre + str(scenic_id),
                                                        product['scenic_product_id'] + '-' + date_str)
                                if price_str:
                                    product_price_dic = json.loads(price_str)
                                    product['product_price']['selling_price'] = product_price_dic['price']
                                else:
                                    product['product_price']['can_book'] = 0
                                product['product_price']['sell_date'] = date_str + ' 00:00:00'
                        delta = datetime.timedelta(days=1)
                        checkInDate = checkInDate + delta

                        # yield meta['ticket_item']
                        ycf_items = YcfspiderItem()
                        ycf_items['kafka_url'] = settings.get('KAFKA_ADDRESS') + settings.get(
                            'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                        ycf_items['results'] = meta['ticket_item']['results']
                        yield ycf_items
                    self.r.delete(redis_count_key_pre + str(scenic_id))
                    self.r.delete(redis_cache_key_pre + str(scenic_id))
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            if response.meta['type'] == 3:
                error_log_dic['id'] = response.meta['package']['package_id']
                error_log_dic['pid'] = response.meta['package']['scenic_id']
            else:
                error_log_dic['id'] = response.meta['product']['scenic_id']
                error_log_dic['pid'] = response.meta['product']['product_id']
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def get_package_info(self, data, package_info, scenic_id, package_id):
        try:
            packages = data['data'].get('packages')
            for package in packages:
                package_dic = {}
                package_dic['id'] = package.get('id')
                package_dic['package_name'] = package.get('name')
                package_dic['price'] = package.get('price')
                package_info.append(package_dic)
        except Exception, e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = package_id
            error_log_dic['pid'] = scenic_id
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def get_package(self, meta, redis_cache_key_pre):
        product_id_list = meta['product_id_list']
        scenic_id = meta['package']['scenic_id']
        results = copy.deepcopy(meta['ticket_item'].get('results'))
        for ticket in meta['ticket_item'].get('results')['tickets']:
            if ticket['scenic_ticket_class_id'] == meta['package']['scenic_ticket_class_id']:
                tickets = []
                for product in ticket['ticket_products']:
                    ticket_new = copy.deepcopy(ticket)
                    ticket_new['scenic_ticket_type_id'] = product['scenic_product_id']
                    ticket_new['scenic_ticket_type_name'] = product['scenic_product_name']
                    ticket_new['ticket_products'] = []
                    key = 'P-%s' % (product['scenic_product_id'])
                    package_info = json.loads(self.r.hget(redis_cache_key_pre + str(scenic_id), key))
                    for package in package_info:
                        product_new = copy.deepcopy(product)
                        product_dic = {}
                        product_new['scenic_product_id'] = str(package['id'])
                        product_new['scenic_product_name'] = package['package_name']
                        product_new['scenic_ticket_type_id'] = ticket_new['scenic_ticket_type_id']
                        product_new['product_price']['scenic_ticket_type_id'] = ticket_new['scenic_ticket_type_id']
                        product_new['product_price']['scenic_product_id'] = product_new['scenic_product_id']
                        product_new['product_price']['selling_price'] = str(package['price'])
                        product_dic['scenic_id'] = product_new['scenic_id']
                        product_dic['product_id'] = package['id']
                        product_dic['type'] = 4
                        product_id_list.append(product_dic)
                        ticket_new['ticket_products'].append(product_new)
                    tickets.append(ticket_new)
                results['tickets'].remove(ticket)
                results['tickets'].extend(tickets)
                meta['ticket_item']['results'] = results
                # yield meta['ticket_item']

    def get_mp_price(self, data, product_id, scenic_id):
        try:
            product_price_list = data['data'].get('priceStocks')
            for product_price in product_price_list:
                product_price_dic = {}
                product_price_dic['date'] = product_price.get('date')
                product_price_dic['stock'] = product_price.get('stock')
                product_price_dic['price'] = product_price.get('price')
                self.price_cache_write(product_id, scenic_id, product_price_dic)
        except Exception, e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = product_id
            error_log_dic['pid'] = scenic_id
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def get_lv_price(self, data, product_id, scenic_id):
        try:
            product_price_list = data['data'].get('priceStocks')
            for product_price in product_price_list:
                product_price_dic = {}
                product_price_dic['date'] = product_price.get('date')
                product_price_dic['stock'] = product_price.get('stock')
                product_price_dic['price'] = product_price.get('price').get('adult') / 100
                self.price_cache_write(product_id, scenic_id, product_price_dic)
        except Exception, e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = product_id
            error_log_dic['pid'] = scenic_id
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def get_tc_price(self, data, product_id, scenic_id):
        try:
            product_price_list = data['data'].get('stocks')
            for product_price in product_price_list:
                product_price_dic = {}
                product_price_dic['date'] = datetime.datetime.fromtimestamp(product_price.get('date') / 1000).strftime(
                    '%Y-%m-%d')
                product_price_dic['stock'] = product_price.get('stock')
                product_price_dic['price'] = product_price.get('price') / 100
                self.price_cache_write(product_id, scenic_id, product_price_dic)
        except Exception, e:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICPIRCE
            error_log_dic['id'] = product_id
            error_log_dic['pid'] = scenic_id
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def price_cache_write(self, product_id, scenic_id, product_price_dic):
        redis_cache_key_pre = 'scenic_price_cache:4_'
        key = '%s-%s' % (product_id, product_price_dic['date'])
        self.r.hset(redis_cache_key_pre + str(scenic_id), key, json.dumps(product_price_dic))

    def download_errback(self, e):
        print type(e), repr(e)

        # def close(spider, reason):
        #     spiderStateRecord.flag_remove(spider.name)
        #     closed = getattr(spider, 'closed', None)
        #     if callable(closed):
        #         return closed(reason)
