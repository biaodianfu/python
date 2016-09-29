# encoding=utf-8

import pymysql
from utils.redisutils import  RedisUtil
from utils.model_logger import Logger
import sys
import traceback

import sys

reload(sys)
sys.setdefaultencoding("utf-8")
import time
import pymongo

class StartMaster():
    def __init__(self):
        self.collection = ''
        self.conf = {}
        self.r = ''
        self.connection = ''
        self.elong_hotel_id = ''
        self.logger = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/ElongPcHotelInfoMasterYcf')
        self.connection = pymongo.MongoClient(self.conf['mongo_host'], int(self.conf['mongo_port']))
        self.elong_hotel_id = self.connection.hotelspiders.elong_pc_hotel_id_all
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'], db=1)
        self.fill_elong_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mysql_connect(self):
        self.collection = pymysql.connect(host=self.conf['mysql_host'], user=self.conf['mysql_user'],
                                          passwd=self.conf['mysql_passwd'], db=self.conf['mysql_db'], charset='utf8')

    def fill_elong_data(self):
        while True:
            try:
                if self.r.len('spider:elong_pc_hotel_info_ycf') == 0 and self.r.len('ElongPcHotelInfoSpider:requests') == 0:
                    self.mysql_connect()
                    hotel_infos_cur = self.collection.cursor()
                    hotel_infos_cur.execute(
                        'select product_id,channel_id,channel_product_id,channel_product_name from ' + self.conf['mysql_hotel_table'])
                    # hotel_infos = hotel_infos_cur.fetchall()
                    count = 1
                    for item in hotel_infos_cur:
                        if (item[1] == 3):
                            if (item[2] != ''):
                                # write_dic = {}
                                # write_dic['hotel_id'] = item[2]
                                # write_dic['hotel_name'] = item[3]
                                # write_dic['ycf_id']=item[0]
                                # for data in self.elong_hotel_id.find({'hotel_id':item[2]},{"hotel_id" : 1,"city_id" : 1,"hotel_name" : 1,"lat" :1,"lng" :1, "hotel_address" :1,"city_namegb" : 1}):
                                #     self.push2redis('spider:elong_pc_hotel_info_ycf', data)
                                for data in self.elong_hotel_id.find({'hotel_id':item[2]}):
                                         write_dic = {}
                                         write_dic["city_name_gb"] = data["city_name_gb"]
                                         write_dic["city_id"] = data["city_id"]
                                         write_dic["hotel_id"] = data["hotel_id"]
                                         write_dic["hotel_address"] = data["hotel_address"]
                                         write_dic["hotel_name"] = data["hotel_name"]
                                         write_dic["lat"] = data["lat"]
                                         write_dic["lng"] = data["lng"]
                                         write_dic["channel"] = data["channel"]
                                         self.push2redis('spider:elong_pc_hotel_info_ycf', write_dic)
                                count += 1
                    hotel_infos_cur.close()
                    self.collection.close()
                    self.logger.info('elong_pc_hotel_info_master_ycf write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception,e:
                    self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)


if __name__ == '__main__':
    master = StartMaster()
    master.run()