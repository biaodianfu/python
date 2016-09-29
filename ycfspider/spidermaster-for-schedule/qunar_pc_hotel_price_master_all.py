#encoding=utf-8

from pymongo import MongoClient
from pymongo.collection import Collection
from utils.redisutils import  RedisUtil
import time
import traceback
from utils.model_logger import Logger
__author__ = 'lizhipeng'

class QunarPcHotelPriceMasterAll():

    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''
        self.logger = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/QunarPcHotelPriceMasterAll')
        self.mongo_connect()
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'])
        self.fill_qunar_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mongo_connect(self):
        self.client = MongoClient(self.conf['mongo_host'], int(self.conf['mongo_port']))
        db = self.client[self.conf['mongo_db']]
        self.collection = Collection(db, 'qunar_pc_hotel_id_all')

    def fill_qunar_data(self):

        while True:
            try:
                if self.r.len('spider:qunar_pc_hotel_price_all') == 0 and self.r.len('QunarPcHotelPriceSpider:requests') == 0:
                    hotel_infos = self.collection.find({})
                    count = 1
                    for item in hotel_infos:
                        write_dic = {}
                        write_dic['hotel_id'] = item['hotel_id']
                        write_dic['hotel_name'] = item['hotel_name']
                        write_dic['count'] = count
                        self.push2redis('spider:qunar_pc_hotel_price_all', write_dic)
                        # if count == 3000:
                        #     break
                        count += 1
                    hotel_infos = None
                    self.client.close()
                    self.logger.info('qunar_pc_hotel_price_master_all write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)


    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = QunarPcHotelPriceMasterAll()
    master.run()