#encoding=utf-8
from pymongo import MongoClient
from pymongo.collection import Collection
import time
import traceback
from utils.redisutils import RedisUtil
from utils.model_logger import Logger

class QunarPcHotelCommentMasterAll():
    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/QunarPcHotelCommentsMasterAll')
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
                if self.r.len('spider:qunar_pc_hotel_comments_all') == 0 and self.r.len('QunarPcHotelCommentsSpider:requests') == 0:
                   # hotel_infos = list(self.collection.find({}))
                    count = 1
                    for item in self.collection.find():
                        write_dic = {}
                        l = item["hotel_id"].split('_')
                        if len(l) == 2:
                            start_url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0], l[1])
                            start_url = start_url.replace('\n', '')
                            write_dic["url"]=start_url
                            write_dic["city"]=l[0]
                            write_dic["id"]=l[1]
                            self.push2redis('spider:qunar_pc_hotel_comments_all', write_dic)
                            write_dic.clear()
                        if len(l) == 3:
                            start_url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0]+"_"+l[1], l[2])
                            start_url = start_url.replace('\n', '')
                            write_dic["url"]=start_url
                            write_dic["city"]=l[0]+"_"+l[1]
                            write_dic["id"]=l[2]
                            self.push2redis('spider:qunar_pc_hotel_comments_all', write_dic)
                            write_dic.clear()
                        count += 1
                    self.client.close()
                    self.logger.info('qunar_pc_hotel_comments_master_all_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception,e:
                 self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = QunarPcHotelCommentMasterAll()
    master.run()