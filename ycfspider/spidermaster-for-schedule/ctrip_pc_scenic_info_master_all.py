# -*- coding: utf-8 -*-
from utils.model_logger import Logger
from utils.redisutils import RedisUtil
from pymongo import MongoClient
from pymongo.collection import Collection
import time
import traceback

class CtripPcScenicInfoMasterAll():
    def __init__(self):
        self.collection=''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf() #装载配置信息conf
        self.logger = Logger(self.conf['log_path']+'/CtripPcScenicInfoMasterAll')
        self.mongo_connect()
        self.r = RedisUtil(self.conf['redis_host'],self.conf['redis_port'])
        self.fill_lvmama_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mongo_connect(self):
        self.client = MongoClient(self.conf['mongo_host'],int(self.conf['mongo_port']))
        db = self.client[self.conf['mongo_db']]
        self.collection = Collection(db,'ctrip_pc_scenic_id_all')

    def fill_lvmama_data(self):
        while True:
            try:
                if self.r.len('spider:ctrip_pc_scenic_info_all') == 0 and self.r.len('spider:CtripPcScenicInfoSpider') ==0: #?
                    count = 1
                    for item in self.collection.find():
                        write_dic = {}
                        write_dic["scenic_name"] = item["scenic_name"]
                        write_dic["scenic_id"] = item["scenic_id"]
                        write_dic["scenic_address"] = item["scenic_address"]
                        write_dic["scenic_province"]=item["scenic_province"]
                        write_dic["scenic_level"] = item["scenic_level"]
                        write_dic["scenic_score"] = item["scenic_score"]
                        write_dic["scenic_characteristic"] = item["scenic_characteristic"]
                        self.push2redis('spider:ctrip_pc_scenic_info_all',write_dic)
                        count += 1
                    self.client.close()
                    self.logger.info('ctrip_pc_scenic_info_master_all_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception,e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self,key,value):
        self.r.push(key,value)

if __name__=='__main__':
    master = CtripPcScenicInfoMasterAll()
    master.run()
