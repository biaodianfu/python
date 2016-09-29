#encoding=utf-8

import pymysql
from utils.redisutils import RedisUtil
import redis
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time,pymongo
import traceback,re
from utils.model_logger import Logger
__author__ = 'lizhipeng'

class LvmamaPcScenicInfoMasterYcf():

    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''
        self.logger = ''

    def run(self):
        self.load_conf()
        self.logger = Logger(self.conf['log_path']+'/LvmamaPcScenicInfoMasterYcf')
        # self.mysql_connect()
        self.r = RedisUtil(self.conf['redis_host'], self.conf['redis_port'], db=1)
        self.fill_lvmamaScenic_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mysql_connect(self):
        self.collection=pymysql.connect(host=self.conf['mysql_host'],user=self.conf['mysql_user'],passwd=self.conf['mysql_passwd'],db=self.conf['mysql_db'],charset='utf8')

    def fill_lvmamaScenic_data(self):
        while True:
            try:
                if self.r.len('spider:lvmama_pc_scenic_info_ycf') == 0 and self.r.len('LvmamaPcScenicInfoSpider:requests') == 0:
                    self.mysql_connect()
                    scenic_infos_cur = self.collection.cursor()
                    # scenic_infos_cur=pymysql.connect(host=self.conf['mysql_host'],user=self.conf['mysql_user'],passwd=self.conf['mysql_passwd'],db=self.conf['mysql_db'],charset='utf8').cursor()
                    scenic_infos_cur.execute('select product_id,channel_id,channel_product_id,channel_product_name, city_name from ' + self.conf['mysql_scenic_table'])
                    # scenic_infos=scenic_infos_cur.fetchall()
                    count = 1
                    ticket_id_record = []
                    for item in scenic_infos_cur:
                        if(item[1]==7):
                            if (item[2]!=''):
                                if item[2] in ticket_id_record:
                                    continue

                                scen_id = re.findall(r'[0-9]+',item[2])
                                if scen_id:
                                        write_dic = {}
                                        write_dic["scenic_id"] = item[2]
                                        ticket_id_record.append( write_dic["scenic_id"])
                                        # print write_dic
                                        self.push2redis('spider:lvmama_pc_scenic_info_ycf', write_dic)
                                        count += 1
                        #         # if count == 3000:
                        #         #     break
                        #         count += 1
                    # print count
                    ticket_id_record = []
                    scenic_infos = None
                    scenic_infos_cur.close()
                    self.collection.close()
                    #self.client.close()
                    self.logger.info('lvmama_pc_scenic_info_ycf write_record data:'+str(count))
                    if self.conf.get('LOOP') == '0':
                        break
            except Exception, e:
                self.logger.error(traceback.format_exc())
            time.sleep(60)

    def push2redis(self, key, value):
        self.r.push(key, value)

if __name__=='__main__':
    master = LvmamaPcScenicInfoMasterYcf()
    master.run()