#encoding=utf-8
import random
import traceback
from pymongo import MongoClient
from pymongo.collection import Collection
from scrapy import log
from scrapy.conf import settings
import json
import requests
import time
from scrapy.http import Response

from ycfspider.utils.model_logger import Logger

__author__ = 'lizhipeng'

class ProxyMiddleware(object):

    def __init__(self):
        self.proxy = ''
        self.logger = ''
        client = MongoClient(settings.get('MONGODB_HOST'), int(settings.get('MONGODB_PORT')))
        db = client[settings.get('MONGODB_DATABASE')]
        self.collection = Collection(db, 'ip_proxy')

    def get_ip_from_mongo(self, channel):
        success_rate = 95
        proxy_list = []
        while True:
            proxy = {'channel': channel,'success_rate': {'$gt': success_rate},'status': 1,'success': {'$gt': 3}, 'continue_failed': 0}
            cursor = self.collection.find(proxy)
            for p in cursor:
                proxy_list.append(p)
            if len(proxy_list) >= 5:
                p = random.choice(proxy_list)
                return p['ip'] + ':' + p['port']
            else:
                success_rate -= 5

    def get_proxy(self, channel):
        # path = settings['PROXY_ADDRESS']+'/getIp?type=meituan'
        path = 1
        if channel == 'QunarPcHotelPriceSpider' or channel =='QunarPcHotelInfoSpider' or channel == 'QunarPcScenicInfoSpider'\
                or channel=='QunarOtaPcZcfHotelInfoSpider' or channel=='QunarOtaPcHotaHotelInfoSpider':
            # path += '?type=quanr'
            path = 2
        elif channel == 'ElongPcHotelPriceSpider' or channel =='ElongPcHotelInfoSpider':
            # path += '?type=elong'
            path = 3
        elif channel == 'CtripMHotelPriceSpider'  or channel =='CtripPcHotelInfoSpider' or channel== 'CtripPcScenicInfoSpider':
            # path += '?type=ctrip'
            path = 1
        elif channel == 'MeituanAppScenicPriceSpider' or channel=='MeituanAppScenicInfoSpider':
            # path += '?type=meituan'
            path = 4
        # proxy = requests.get(path).content
        # self.proxy = 'http://'+proxy
        self.proxy = 'http://'+self.get_ip_from_mongo(path)
        # self.proxy = 'http://124.88.67.7:81'
        log.msg(self.proxy, level=log.INFO)
        return self.proxy


    # # overwrite process request
    def process_request(self, request, spider):
        # Set the location of the proxy
        if self.logger == '':
            self.logger = Logger(settings['LOG_PATH']+'/'+spider.name+'/proxy_error/')
        # if self.proxy == '' or request.meta.get('retry_times', 0) > 0:
        try:
            request.meta['proxy'] = self.get_proxy(spider.name)
        except:
            request.meta['proxy'] = 'http://120.52.73.34:8081'
            print traceback.format_exc()



    def process_exception(self, request, exception, spider):
        try:
            request.meta['proxy'] = self.get_proxy(spider.name)
        except:
            request.meta['proxy'] = 'http://120.52.73.34:8081'
            print traceback.format_exc()
        if request.meta.get('retry_times', 0) == settings.getint('RETRY_TIMES'):
            self.logger.info('proxy retry '+str(request.meta.get('retry_times', 0))+' times fail, spider:'+
                             spider.name + ' url:' + request.url)
        response = Response(url=request.url, headers=request.headers, body='', request=request, status=200)
        return response


if __name__=='__main__':
    p = ProxyMiddleware()