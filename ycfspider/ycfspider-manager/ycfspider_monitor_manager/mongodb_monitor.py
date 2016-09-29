#encoding=utf-8

import os
import ConfigParser
import time

from pymongo import MongoClient
from pymongo.collection import Collection

from utils.send_email_utils import server
from utils.model_logger import Logger

__author__ = 'lizhipeng'

class MongodbManager(object):

    def __init__(self):
        self.cf = None
        self.load_conf()
        self.collection = None
        self.mongo_connect()
        self.logger = Logger(self.cf.get('log', 'log_path') + '/mongodb_monitor')

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

    def monitor(self):
        while True:
            result = self.collection.find({}).count()
            self.logger.info(u'mongodb中的kafka的失败缓存数量为：' + str(result))
            # print u'mongodb中的kafka的失败缓存数量为：' + str(result)
            if result is not 0:
                email_receviers = ['641785844@qq.com']
                self.logger.info(u'接收警报的邮箱为：' + email_receviers.__str__())
                msg = 'mongodb中的kafka的失败缓存数量为：' + str(result) + '条，请注意！！！！'
                server.send_email(email_receviers, msg)
                self.logger.info(u'邮件发送成功')
            time.sleep(300)





manager = MongodbManager()
manager.monitor()

