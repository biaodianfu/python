# -*- coding: utf-8 -*-
import json
import traceback

import requests
import time

from utils.model_logger import Logger
from utils.redisutils import RedisUtil



class CtripOtaPcEbookingHotelInfoMasterAll():
    def __init__(self):
        self.collection=''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf() #装载配置信息conf
        self.logger = Logger(self.conf['log_path']+'/CtripOtaPcEbookingHotelInfoMaster')
        # self.collection=pymysql.connect(host=settings.get('LOGIN_MYSQL_HOST'),port=settings.get('LOGIN_MYSQL_PORT'),user=settings.get('LOGIN_MYSQL_USERNAME'),passwd=settings.get('LOGIN_MYSQL_USERPWD'),db=settings.get('LOGIN_MYSQL_DB'),charset='utf8')
        # self.sel_user_message = "select * from "+settings.get('LOGIN_MYSQL_TABLE')+" where  = '"+user_name+"';"
        self.r = RedisUtil(self.conf['redis_host'],self.conf['redis_port'])
        self.fill_quna_hota_usr_pwd_data()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def fill_quna_hota_usr_pwd_data(self):
        while True:
            try:
                if self.r.len('spider:ctrip_ota_ebook_user_pwd_master') == 0 and self.r.len('CtripOtaPcEbookingHotelInfoSpider:requests') ==0: #?
                    count = 1
                    # params=None,data=None,headers=None,cookies=None,files=None,auth=None,timeout=None,allow_redirects=True,proxies=None,hooks=None,stream=None,verify=None,cert=None,json=None
                    url = self.conf['ota_address'] + '/priceratio/common/product/otausermsg'
                    data = {"channelId":147}
                    user_pwd_list = requests.post(url,params=data)
                    user_pwd_list = user_pwd_list.content
                    #将获取的数据按照对应字段组织起来写入redis
                    if user_pwd_list:
                        data = json.loads(user_pwd_list).get('data')
                        if data:
                            for item in data:
                                #print type(item)
                                 write_dic = {}
                                 write_dic['channel_id'] = item['channelId']
                                 write_dic['channel_name'] = item['channelName']
                                 write_dic['region'] = item['cityName']
                                 write_dic['channel_number'] = 'QDFZ'+str(item['channelSubInfoId'])
                                 write_dic['login_url'] = item['tuangouSubUrl']
                                 write_dic['user_name'] = item['loginAccount']
                                 write_dic['password'] = item['loginPassword']
                                 if len(write_dic) == 7:
                                    # print write_dic
                                    self.push2redis('spider:ctrip_ota_ebook_user_pwd_master',write_dic)
                                 count += 1
                            # if count == len(data):
                            self.logger.info('ctrip_ota_ebook_user_pwd_master_record data:'+str(count))
                            if self.conf.get('LOOP') == '0':
                                break
            except Exception,e:
                self.logger.error(traceback.format_exc())
            time.sleep(12*3600)

    def push2redis(self,key,value):
        self.r.push(key,value)

if __name__=='__main__':
    master = CtripOtaPcEbookingHotelInfoMasterAll()
    master.run()
