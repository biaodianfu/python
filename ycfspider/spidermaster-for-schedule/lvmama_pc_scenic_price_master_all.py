#encoding=utf-8

from pymongo import MongoClient
from pymongo.collection import Collection
from utils.redisutils import  RedisUtil
import time
import traceback
from utils.model_logger import Logger
__author__ = 'lizhipeng'

class LvmamaPcScenicPriceMasterAll():

    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/LvmamaPcScenicPriceMasterAll')
        self.mongo_connect()
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'])
        self.fill_ctrip_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mongo_connect(self):
        self.client = MongoClient(self.conf['mongo_host'], int(self.conf['mongo_port']))
        db = self.client[self.conf['mongo_db']]
        self.collection = Collection(db, 'lvmama_pc_scenic_id_all')

    def fill_ctrip_data(self):
        while True:
            try:
                if self.conf.get('LOOP') == '0':
                    if not self.r.get('spider_flag_record:LvmamaPcScenicPriceSpider:all'):
                        cache_key = self.r.check_key('scenic_price_cache:7*')
                        count_key = self.r.check_key('scenic_price_count:7*')
                        for key in cache_key:
                            self.r.delete(key)
                        for key in count_key:
                            self.r.delete(key)
                if self.r.len('spider:lvmama_pc_scenic_price_all') == 0 and self.r.len('LvmamaPcScenicPriceSpider:requests') == 0\
                        and len(self.r.check_key('scenic_price_cache:7*')) == 0 and len(self.r.check_key('scenic_price_count:7*')) == 0:
                    hotel_infos = self.collection.find({})
                    count = 1
                    for item in hotel_infos:
                        write_dic = {}
                        write_dic['city_id'] = item['city_id']
                        write_dic['scenic_id'] = item['scenic_id']
                        write_dic['city_name'] = item['city_name']
                        write_dic['scenic_name'] = item['scenic_name']
                        write_dic["address" ] = item["address" ]
                        write_dic["city_pinyin_name" ] = item["city_pinyin_name" ]
                        write_dic['count'] = count
                        self.push2redis('spider:lvmama_pc_scenic_price_all', write_dic)
                        # if count == 3000:
                        #     break
                        count += 1
                    hotel_infos = None
                    self.client.close()
                    self.logger.info('lvmama_pc_scenic_price_master_all write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = LvmamaPcScenicPriceMasterAll()
    master.run()