#encoding=utf-8
from pymongo import MongoClient
from pymongo.collection import Collection
import time
import traceback
from utils.redisutils import RedisUtil
from utils.model_logger import Logger

class ElongPcHotelCommentMasterAll():
    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/ElongPcHotelCommentMasterAll')
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
        self.collection = Collection(db, 'elong_pc_hotel_id_all')


    def fill_ctrip_data(self):
        while True:
            try:
                if self.r.len('spider:elong_pc_hotel_comment_all') == 0 and self.r.len('ElongPcHotelCommentSpider:requests') == 0:
                   # hotel_infos = list(self.collection.find({}))
                    count = 1
                    for item in self.collection.find():
                        write_dic = {}
                        write_dic["hotel_id" ] = item["hotel_id" ]
                        write_dic["city_id" ] = item["city_id" ]
                        write_dic["hotel_name" ] = item["hotel_name" ]
                        write_dic["lat" ] = item["lat" ]
                        write_dic["lng" ] = item["lng" ]
                        write_dic["hotel_address" ] = item["hotel_address" ]
                        write_dic["city_name_gb" ] = item["city_name_gb" ]
                        write_dic["channel"]=item["channel"]
                        self.push2redis('spider:elong_pc_hotel_comment_all', write_dic)
                        count += 1
                    self.client.close()
                    self.logger.info('elong_pc_hotel_comment_master_all_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                 self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
       self.r.push(key, value)

if __name__=='__main__':
    master = ElongPcHotelCommentMasterAll()
    master.run()