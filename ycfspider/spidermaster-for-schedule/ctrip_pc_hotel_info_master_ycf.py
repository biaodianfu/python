#encoding=utf-8

#import MySQLdb
import pymysql
import redis
from utils.redisutils import RedisUtil
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from utils.model_logger import Logger
import traceback
import time

class StartMaster():

    def __init__(self):
        self.collection = ''
        self.conf = {}
        self.logger = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/CtripPcHotelInfoMasterYcf')
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'], db=1)
        self.fill_ctrip_data()


    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mysql_connect(self):
        self.collection=pymysql.connect(host=self.conf['mysql_host'],user=self.conf['mysql_user'],passwd=self.conf['mysql_passwd'],db=self.conf['mysql_db'],charset='utf8')

    def fill_ctrip_data(self):
        while True:
            try:
                if self.r.len('spider:ctrip_pc_hotel_info_ycf') == 0 and self.r.len('CtripPcHotelInfoSpider:requests') == 0:
                    self.mysql_connect()
                    hotel_infos_cur = self.collection.cursor()
                    hotel_infos_cur.execute('select product_id,channel_id,channel_product_id,channel_product_name from ' + self.conf['mysql_hotel_table'])
                    # hotel_infos=hotel_infos_cur.fetchall()
                    count = 1
                    for item in hotel_infos_cur:
                        if(item[1]==1):
                            if (item[2]!=''):
                                write_dic = {}
                                write_dic['hotel_id'] = item[2]
                                write_dic['hotel_name'] = item[3]
                                write_dic['ycf_id']=item[0]
                                self.push2redis('spider:ctrip_pc_hotel_info_ycf', write_dic)
                                count += 1
                    hotel_infos = None
                    hotel_infos_cur.close()
                    self.collection.close()
                    self.logger.info('ctrip_pc_hotel_info_master_ycf write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception,e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = StartMaster()
    master.run()