#encoding=utf-8

import pymysql
from utils.redisutils import  RedisUtil
import redis
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time
from utils.model_logger import Logger
import traceback
__author__ = 'lizhipeng'

class CtripPcScenicPriceMasterYcf():

    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''
        self.logger = ''


    def run(self):
        self.load_conf()
        # self.mysql_connect()
        self.logger = Logger(self.conf['log_path']+'/CtripPcScenicPriceMasterYcf')
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'], db=1)
        self.fill_ctripScenic_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mysql_connect(self):
        self.collection=pymysql.connect(host=self.conf['mysql_host'],user=self.conf['mysql_user'],passwd=self.conf['mysql_passwd'],db=self.conf['mysql_db'],charset='utf8')

    def fill_ctripScenic_data(self):
        while True:
            try:
                if self.conf.get('LOOP') == '0':
                    if not self.r.get('spider_flag_record:CtripPcScenicPriceSpider:ycf'):
                        cache_key = self.r.check_key('scenic_price_cache:1*')
                        count_key = self.r.check_key('scenic_price_count:1*')
                        for key in cache_key:
                            self.r.delete(key)
                        for key in count_key:
                            self.r.delete(key)
                if self.r.len('spider:ctrip_pc_scenic_price_ycf') == 0 and self.r.len('CtripPcScenicPriceSpider:requests') == 0 \
                        and len(self.r.check_key('scenic_price_cache:1*')) == 0 and len(self.r.check_key('scenic_price_count:1*')) == 0:
                    # scenic_infos_cur = self.collection.cursor()
                    self.mysql_connect()
                    scenic_infos_cur= self.collection.cursor()
                    scenic_infos_cur.execute('select product_id,channel_id,channel_product_id,channel_product_name, city_name from ' + self.conf['mysql_scenic_table'])
                    # scenic_infos=scenic_infos_cur.fetchall()
                    count = 1
                    id_record = []
                    for item in scenic_infos_cur:
                        if(item[1]==1):
                            if (item[2]!=''):
                                if  item[2] in id_record:
                                    continue
                                write_dic = {}
                                write_dic['scenic_id'] = item[2]
                                id_record.append(item[2])
                                # write_dic['hotel_name'] = item['hotelname']
                                write_dic['scenic_name'] = item[3]
                                write_dic['ycf_id']=item[0]
                                url = "http://piao.ctrip.com/dest/"+write_dic["scenic_id"]+".html"
                                write_dic["url"] = url
                                self.push2redis('spider:ctrip_pc_scenic_price_ycf', write_dic)
                                # if count == 3000:
                                #     break
                                count += 1
                    scenic_infos = None
                    scenic_infos_cur.close()
                    self.collection.close()
                    #self.client.close()
                    self.logger.info('ctrip_pc_scenic_price_master_ycf write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = CtripPcScenicPriceMasterYcf()
    master.run()