#encoding=utf-8
from twisted.internet import reactor

__author__ = 'lizhipeng'

from pymongo import MongoClient
from scrapy.conf import settings
from scrapy import log
import datetime

import json
import traceback
import requests
from ycfspider.utils.model_logger import Logger

class KafkaPipeline(object):

    def __init__(self):
        self.client = MongoClient(settings[ 'MONGODB_HOST' ], settings[ 'MONGODB_PORT' ])
        self.db =self.client[settings[ 'MONGODB_DATABASE' ]]
        self.logger_failed = ''
        self.logger_data = ''
        # self.logger_id = ''
        self.grep_char = ['\n', '\r', '\t']

    def process_item(self, item, spider):
        if 'HotelPrice' in  spider.name:
            results = item['results']
            rooms = results['room']
            for room in rooms:
                kafka_data = {}
                kafka_data['hotel_id'] = results['hotel_id']
                kafka_data['room'] = []
                kafka_data['room'].append(room)
                kafka_data_str = json.dumps(kafka_data).replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
                kafka_data_str = kafka_data_str.replace('\\n', '').replace('\\r', '').replace('\\t', '')
                self.log_kafka(item, spider, kafka_data_str)
                reactor.callInThread(self.write_2_kafka,kafka_data_str, item, spider)
        elif 'ScenicPrice' in spider.name :
            results = item['results']
            tickets = results['tickets']
            for ticket in tickets:
                kafka_data = {}
                kafka_data['scenic_id'] = results['scenic_id']
                kafka_data['scenic_name'] = results['scenic_name']
                kafka_data['channel_id'] = results['channel_id']
                kafka_data['platform_id'] = results['platform_id']
                kafka_data['city_name_cn'] = results['city_name_cn']
                kafka_data['tickets'] = []
                kafka_data['tickets'].append(ticket)
                kafka_data_str = json.dumps(kafka_data).replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
                kafka_data_str = kafka_data_str.replace('\\n', '').replace('\\r', '').replace('\\t', '')
                self.log_kafka(item, spider, kafka_data_str)
                reactor.callInThread(self.write_2_kafka,kafka_data_str, item, spider)
        else:
            data = json.dumps(dict(item['results']))
            data = data.replace('\\n', '').replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
            data = data.replace('\\n', '').replace('\\r', '').replace('\\t', '')
            self.log_kafka(item, spider, data)
            reactor.callInThread(self.write_2_kafka,data, item, spider)
        return ''

    def write_2_kafka(self, data, item, spider):
        # 写入kafka接口，如果失败，重试三次，重试三次失败后放弃，交给下一个写入方式。
        result_flag = False
        status_code = None
        content = None
        do_count = 3
        while do_count:
            try:
                header = {"Content-Type": "application/json; charset=utf8"}
                url = item.get('kafka_url')
                r = requests.post(url, data=data, headers=header,timeout=20)
                status_code = r.status_code
                content = r.content
                if r.status_code == 200:
                    result = r.json()
                else:
                    raise Exception('kafka http error')
                if int(result['rescode']) == 1:
                    result_flag = True
                    # collection_name = spider.name
                    # collection = self.db[collection_name]
                    # timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # collection.insert(
                    #     {'timestamp': timestamp, 'data': data, 'hotel_id': item['hotel_id'],
                    #      'hotel_name': item['hotel_name']})
                    log.msg(u'写入kafka成功', level=log.INFO, spider=spider.name)
                    break
            except Exception, e:
                log.msg('kafka status code:' + str(status_code), level=log.ERROR, spider=spider.name)
                log.msg(content, level=log.ERROR, spider=spider.name)
                log.msg(traceback.format_exc(), level=log.ERROR, spider=spider.name)
            do_count -= 1
        if not result_flag:
            log.msg(u'写入kafka失败', level=log.ERROR, spider=spider.name)
            self.write_2_mongo(data, item, spider)
        # return result_flag

    def log_kafka(self, item, spider , log_data):
        if self.logger_data == '':
            self.logger_data = Logger(settings['LOG_PATH']+'/'+spider.name+'/kafka/')
        # data = json.dumps(dict(item['results']))
        # data = data.replace('\\n', '').replace('\\r', '').replace('\\t', '')
        # self.logger_data.info(spider.name + ' kafka data:'+data.replace('%', '%%'))
        self.logger_data.info(log_data.replace('%', '%%'))

    def write_2_mongo(self, data, item, spider):
        # 写入mongo，如果失败，重试三次，重试三次失败后放弃，交给下一个写入方式。
        collection_name = 'fail_write2kafka_data'
        collection = self.db[collection_name]
        result_flag = False
        try:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            collection.insert({'spider_name': spider.name, 'timestamp': timestamp, 'data': data})
            result_flag = True
            log.msg(u'写入mongo成功', level=log.INFO, spider=spider.name)
        except Exception, e:
            print e
        if not result_flag:
            log.msg(u'写入mongo失败', level=log.ERROR, spider=spider.name)
            self.write_2_log(data, spider)

    def write_2_log(self, data, spider):
        # 将失败的数据写入日志
        if self.logger_failed == '':
            self.logger_failed = Logger(settings['LOG_PATH']+'/'+spider.name+'/failed/')
        self.logger_failed.info(data.replace('%', '%%'))
