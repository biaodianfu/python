#encoding=utf-8

import Queue
import os
import ConfigParser
import threading
import traceback
import json
import re

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
import requests

from ycfspider_monitor_manager.utils.model_logger import Logger

__author__ = 'lizhipeng'


class Producer(object):

    def __init__(self, job_queue, cf, collection, logger):
        self.job_queue = job_queue
        self.cf = cf
        self.collection = collection
        self.logger = logger

    def run(self):
        self.load_id()

    def load_id(self):
            id_list = self.collection.find({}, {"_id": True}).limit(1000)
            self.logger.info('failed write to kafka data count:' + str(id_list.count()))
            for mongo_id in id_list:
                self.job_queue.put(mongo_id['_id'].__str__())
            threads = []
            for i in range(15):
                consumer = Comsumer(self.job_queue, self.cf, self.collection, self.logger)
                threads.append(consumer)
            for thread in threads:
                thread.start()


class Comsumer(threading.Thread):

    def __init__(self, job_queue, cf, collection, logger):
        threading.Thread.__init__(self)
        self.job_queue = job_queue
        self.collection = collection
        self.cf = cf
        self.logger = logger

    def run(self):
        while True:
            if self.job_queue.qsize() > 0:
                self.process_job(self.job_queue.get())
            else:
                break

    def process_job(self, mongo_id):
        upload_data = list(self.collection.find({'_id': ObjectId(mongo_id)}, {'spider_name': True, 'data' : True, '_id': False}))[0]
        kafka_url = self.cf.get('kafka', 'KAFKA_ADDRESS')
        try:
            if 'HotelPrice' in upload_data['spider_name']:
                kafka_url += self.cf.get('kafka', 'KAFKA_HOTEL_PRICE_RESOURSE_PATH')
                # regex = re.compile(r'\\(?![/u"])')
                # fixed = regex.sub(r"\\\\", upload_data['data'])
                results = json.loads(upload_data['data'])
                # results = json.loads(upload_data['data'])
                rooms = results['room']
                for room in rooms:
                    kafka_data = {}
                    kafka_data['hotel_id'] = results['hotel_id']
                    kafka_data['room'] = []
                    kafka_data['room'].append(room)
                    kafka_data_str = json.dumps(kafka_data).replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
                    kafka_data_str = kafka_data_str.replace('\\n', '').replace('\\r', '').replace('\\t', '')
                    self.write_2_kafka(kafka_data_str, mongo_id, kafka_url)
            elif 'ScenicPrice' in upload_data['spider_name']:
                kafka_url += self.cf.get('kafka', 'KAFKA_SCENIC_PRICE_RESOURSE_PATH')
                # regex = re.compile(r'\\(?![/u"])')
                # fixed = regex.sub(r"\\\\", upload_data['data'])
                # results = json.loads(fixed)
                results = json.loads(upload_data['data'])
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
                    self.write_2_kafka(kafka_data_str, mongo_id, kafka_url)
            elif 'HotelInfo' in upload_data['spider_name']:
                kafka_url += self.cf.get('kafka', 'KAFKA_HOTEL_INFO_RESOURSE_PATH')
                kafka_data_str = upload_data['data'].replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
                kafka_data_str = kafka_data_str.replace('\\n', '').replace('\\r', '').replace('\\t', '')
                self.write_2_kafka(kafka_data_str, mongo_id, kafka_url)
            elif 'ScenicInfo' in upload_data['spider_name']:
                kafka_url += self.cf.get('kafka', 'KAFKA_SCENIC_INFO_RESOURSE_PATH')
                kafka_data_str = upload_data['data'].replace('\\\\n', '').replace('\\\\r', '').replace('\\\\t', '')
                kafka_data_str = kafka_data_str.replace('\\n', '').replace('\\r', '').replace('\\t', '')
                self.write_2_kafka(kafka_data_str, mongo_id, kafka_url)
        except:
            log_data = {}
            log_data['data'] = upload_data['data']
            log_data['spider_name'] = upload_data['spider_name']
            log_data['error_info'] = traceback.format_exc()
            self.logger.error(json.dumps(log_data))

    def write_2_kafka(self, data, mongo_id, kafka_url):
        # 写入kafka接口，如果失败，重试三次，重试三次失败后放弃，交给下一个写入方式。
        result_flag = False
        do_count = 3
        while do_count:
            try:
                header = {"Content-Type": "application/json; charset=utf8"}
                url = kafka_url
                r = requests.post(url, data=data, headers=header,timeout=20)
                result = r.json()
                if int(result['rescode']) == 1:
                    result_flag = True
                    # print u'kafka success'
                    # self.logger.info('kafka success')
                    #在mongodb删除记录
                    self.collection.remove({'_id': ObjectId(mongo_id)})
                    break
            except :
                # print traceback.format_exc()
                log_data  = {}
                log_data['kafka_url'] = kafka_url
                log_data['error_info'] = traceback.format_exc()
                self.logger.error(json.dumps(log_data))
            do_count -= 1
        if not result_flag:
            # print u'kafka error'
            self.logger.info('kafka failed')
            # 将id重新放入队列
            # self.job_queue.put(mongo_id)



class UploadDataToKafka(object):

    def __init__(self):
        self.cf = None
        self.load_conf()
        self.collection = None
        self.mongo_connect()
        self.logger = Logger(self.cf.get('log', 'log_path') + '/kafka_manager')

    def load_conf(self):
        current_path = os.getcwd()
        parent_path = os.path.dirname(current_path)
        conf_path = parent_path.replace('\\', '/') + '/conf/manager.conf'
        cf = ConfigParser.ConfigParser()
        cf.read(conf_path)
        self.cf = cf

    def mongo_connect(self):
        client = MongoClient(self.cf.get('mongo', 'mongo_host'), self.cf.getint('mongo', 'mongo_port'))
        db = client[self.cf.get('mongo', 'mongo_db')]
        self.collection = Collection(db, 'fail_write2kafka_data')

    def run(self):
        job_queue = Queue.Queue()
        p = Producer(job_queue, self.cf, self.collection, self.logger)
        p.run()



if __name__=='__main__':
    kafka = UploadDataToKafka()
    kafka.run()
