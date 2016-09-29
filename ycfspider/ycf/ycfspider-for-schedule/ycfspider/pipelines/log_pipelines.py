#encoding=utf-8


__author__ = 'lizhipeng'

import json
import traceback
from datetime import datetime

from twisted.internet import reactor
from scrapy.conf import settings
from scrapy import log
from pymongo import MongoClient

from ycfspider.utils.model_logger import Logger


def enum(**enums):
    return type('Enum', (), enums)
class LogPipelines(object):

    def __init__(self):
        # Channel = enum(CTRIP=1, QUNAR=2, ELONG=3, MEITUAN=4, LVMAMA=7)
        # Platform = enum(PC=1, APP=2, M=3)
        self.Crawltype = enum(HOTELINFO=1, HOTELPRICE=2, SCENICINFO=3, SCENICPIRCE=4)
        self.logger_data = ''
        self.logger_product_id = ''
        self.logger_succeed_id = ''
        self.client = MongoClient(settings[ 'MONGODB_HOST' ], settings[ 'MONGODB_PORT' ])
        self.db =self.client[settings[ 'MONGODB_DATABASE' ]]

    def process_item(self, item, spider):
        # self.log_kafka(item, spider)
        # self.log_product_id(item, spider)
        # self.log_succeed_id(item, spider)
        reactor.callInThread(self.log_product_id, item, spider)
        reactor.callInThread(self.log_succeed_id, item, spider)
        return item



    def log_kafka(self, item, spider):
        if self.logger_data == '':
            self.logger_data = Logger(settings['LOG_PATH']+'/'+spider.name+'/kafka/')
        data = json.dumps(dict(item['results']))
        data = data.replace('\\n', '').replace('\\r', '').replace('\\t', '')
        # self.logger_data.info(spider.name + ' kafka data:'+data.replace('%', '%%'))
        self.logger_data.info(data.replace('%', '%%'))

    def log_product_id(self, item, spider):
        try:
            if self.logger_product_id == '':
                self.logger_product_id = Logger(settings['LOG_PATH'] + '/' + spider.name + '/product_id/')
            collection_name = 'product_id_record'
            collection = self.db[collection_name]
            results = item['results']
            if 'HotelPrice' in spider.name:
                for room in results['room']:
                    type_id = 1
                    platform_id = room['platform_id']
                    channel_id = room['channel_id']
                    property_id = room['hotel_id']
                    property_class_id = ''
                    property_affiliated_id = room['room_type_id']
                    for product in room['sub_rooms']:
                        agent_id = product['agent_id']
                        package_id = product['product_id']
                        crawl_time = product['crawl_time']
                        write_dic = {}
                        write_dic['type_id'] = type_id
                        write_dic['platform_id'] = platform_id
                        write_dic['channel_id'] = channel_id
                        write_dic['property_id'] = property_id
                        write_dic['property_class_id'] = property_class_id
                        write_dic['property_affiliated_id'] = property_affiliated_id
                        write_dic['agent_id'] = agent_id
                        write_dic['package_id'] = package_id
                        write_dic['crawl_time'] = crawl_time
                        write_dic['scale'] = settings.get('SCALE')
                        # 日志记录产品id
                        self.logger_product_id.info(json.dumps(write_dic))
                        write_dic['timestamp'] = today = datetime.now()
                        pattern_dic = {
                            'type_id': type_id,
                            # 'platform_id': platform_id,
                            'channel_id': channel_id,
                            'property_id': property_id,
                            # 'property_class_id': property_class_id,
                            # 'property_affiliated_id': property_affiliated_id,
                            # 'agent_id': agent_id,
                            'package_id': package_id,
                            'timestamp': {'$gte': datetime(today.year, today.month, today.day)}
                        }
                        # 产品id存储mongo，并按天进行去重处理
                        # if len(list(collection.find(pattern_dic))) == 0:
                        #     collection.insert(write_dic)
            elif 'ScenicPrice' in spider.name:
                for ticket in results['tickets']:
                    type_id = 2
                    platform_id = ticket['platform_id']
                    channel_id = ticket['channel_id']
                    property_id = ticket['scenic_id']
                    property_class_id = ticket.get('scenic_ticket_class_id', '')
                    property_affiliated_id = ticket.get('scenic_ticket_type_id', '')
                    for product in ticket['ticket_products']:
                        agent_id = product['agent_id']
                        package_id = product['scenic_product_id']
                        crawl_time = product['crawl_time']
                        write_dic = {}
                        write_dic['type_id'] = type_id
                        write_dic['platform_id'] = platform_id
                        write_dic['channel_id'] = channel_id
                        write_dic['property_id'] = property_id
                        write_dic['property_class_id'] = property_class_id
                        write_dic['property_affiliated_id'] = property_affiliated_id
                        write_dic['agent_id'] = agent_id
                        write_dic['package_id'] = package_id
                        write_dic['crawl_time'] = crawl_time
                        write_dic['scale'] = settings.get('SCALE')
                        # 日志记录产品id
                        self.logger_product_id.info(json.dumps(write_dic))
                        write_dic['timestamp'] = today = datetime.now()
                        pattern_dic = {
                            'type_id': type_id,
                            # 'platform_id': platform_id,
                            'channel_id': channel_id,
                            'property_id': property_id,
                            # 'property_class_id': property_class_id,
                            # 'property_affiliated_id': property_affiliated_id,
                            # 'agent_id': agent_id,
                            'package_id': package_id,
                            'timestamp': {'$gte': datetime(today.year, today.month, today.day)}
                        }
                        # 产品id存储mongo，并按天进行去重处理
                        # if len(list(collection.find(pattern_dic))) == 0:
                        #     collection.insert(write_dic)
            log.msg(u'产品id记录成功', level=log.INFO, spider=spider.name)
        except Exception, e:
            print traceback.format_exc()

    def log_succeed_id(self, item, spider):
        # 日志记录成功抓取的id
        try:
            if self.logger_succeed_id == '':
                self.logger_succeed_id = Logger(settings['LOG_PATH'] + '/' + spider.name + '/succeed_id/')
            collection_name = 'succeed_id_record'
            collection = self.db[collection_name]
            results = item['results']
            if 'HotelPrice' in spider.name:
                log_data = {}
                log_data['id'] = results['hotel_id']
                log_data['platform_id']= ''
                log_data['channel_id'] = ''
                log_data['type_id'] = self.Crawltype.HOTELPRICE
                log_data['crawl_time'] = ''
                if len(results['room']) is not 0:
                    log_data['platform_id'] = results['room'][0]['platform_id']
                    log_data['channel_id'] = results['room'][0]['channel_id']
                    log_data['crawl_time'] = results['room'][0]['crawl_time']
                log_data['scale'] = settings.get('SCALE')
                self.logger_succeed_id.info(json.dumps(log_data))
                log_data['timestamp'] = today = datetime.now()
                pattern_dic = {
                    'id': log_data['id'],
                    # 'platform_id': log_data['platform_id'],
                    'channel_id': log_data['channel_id'],
                    'type_id': log_data['type_id'],
                    'timestamp': {'$gte': datetime(today.year, today.month, today.day)}

                }
                # if len(list(collection.find(pattern_dic))) == 0:
                #     collection.insert(log_data)
            elif 'ScenicPrice' in spider.name:
                log_data = {}
                log_data['id'] = results['scenic_id']
                log_data['platform_id']= results['platform_id']
                log_data['channel_id'] = results['channel_id']
                log_data['type_id'] = self.Crawltype.SCENICPIRCE
                log_data['crawl_time'] = ''
                if len(results['tickets']) is not 0:
                    log_data['crawl_time'] = results['tickets'][0]['crawl_time']
                log_data['scale'] = settings.get('SCALE')
                self.logger_succeed_id.info(json.dumps(log_data))
                log_data['timestamp'] = today = datetime.now()
                pattern_dic = {
                    'id': log_data['id'],
                    # 'platform_id': log_data['platform_id'],
                    'channel_id': log_data['channel_id'],
                    'type_id': log_data['type_id'],
                    'timestamp': {'$gte': datetime(today.year, today.month, today.day)}

                }
                # if len(list(collection.find(pattern_dic))) == 0:
                #     collection.insert(log_data)
            elif 'HotelInfo' in spider.name and 'Ota' not in spider.name:
                log_data = {}
                log_data['id'] = results['hotel_id']
                log_data['platform_id']= results['platform_id']
                log_data['channel_id'] = results['channel_id']
                log_data['type_id'] = self.Crawltype.HOTELINFO
                log_data['crawl_time'] = results['crawl_time']
                log_data['scale'] = settings.get('SCALE')
                self.logger_succeed_id.info(json.dumps(log_data))
                log_data['timestamp'] = today = datetime.now()
                pattern_dic = {
                    'id': log_data['id'],
                    # 'platform_id': log_data['platform_id'],
                    'channel_id': log_data['channel_id'],
                    'type_id': log_data['type_id'],
                    'timestamp': {'$gte': datetime(today.year, today.month, today.day)}

                }
                # if len(list(collection.find(pattern_dic))) == 0:
                #     collection.insert(log_data)
            elif 'ScenicInfo' in spider.name:
                log_data = {}
                log_data['id'] = results['scenic_id']
                log_data['platform_id']= results['platform_id']
                log_data['channel_id'] = results['channel_id']
                log_data['type_id'] = self.Crawltype.SCENICINFO
                log_data['crawl_time'] = results['crawl_time']
                log_data['scale'] = settings.get('SCALE')
                self.logger_succeed_id.info(json.dumps(log_data))
                log_data['timestamp'] = today = datetime.now()
                pattern_dic = {
                    'id': log_data['id'],
                    # 'platform_id': log_data['platform_id'],
                    'channel_id': log_data['channel_id'],
                    'type_id': log_data['type_id'],
                    'timestamp': {'$gte': datetime(today.year, today.month, today.day)}

                }
                # if len(list(collection.find(pattern_dic))) == 0:
                #     collection.insert(log_data)

            log.msg(u'爬取id记录成功', level=log.INFO, spider=spider.name)
        except:
            print traceback.format_exc()



