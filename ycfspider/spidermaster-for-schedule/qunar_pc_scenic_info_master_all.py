#encoding=utf-8
from pymongo import MongoClient
from pymongo.collection import Collection
import time
import traceback
from utils.redisutils import RedisUtil
from utils.model_logger import Logger

class QunarPcHotelInfoMasterAll():
    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/QunarPcScenicInfoMasterAll')
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
        self.collection = Collection(db, 'qunar_pc_scenic_id_all')


    def fill_ctrip_data(self):
        while True:
            try:
                if self.r.len('spider:qunar_pc_scenic_info_all') == 0 and self.r.len('spider:QunarPcScenicInfoSpider') == 0:
                    #  hotel_infos = list(self.collection.find({}))
                    count = 1
                    for item in self.collection.find():
                        write_dic = {}
                        write_dic["scenic_name"]=item["scenic_name"]
                        write_dic["city_name_cn"]=item["city_name_cn"]
                        write_dic["scenic_id"]=item["scenic_id"]
                        write_dic["address"]=item["address"]
                        write_dic["qunar_price"]=item["qunar_price"]
                        self.push2redis('spider:qunar_pc_scenic_info_all', write_dic)
                        count += 1
                    self.client.close()
                    self.logger.info('qunar_pc_scenic_info_master_all_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                self.logger.error(traceback.format_exc())
            # time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = QunarPcHotelInfoMasterAll()
    master.run()