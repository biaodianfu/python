#coding=utf-8
import requests,json
from scrapy.conf import settings
import pymongo,re,random,time
from pymongo import MongoClient
import requests,urllib
user_list = [
    'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
    'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
    'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10',
    'Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/1A542a Safari/419.3',
    'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)'
]
class ProxyUtil(object):

    def __init__(self):
        self.client = MongoClient(settings.get('MONGODB_HOST'), int(settings.get('MONGODB_PORT')))
        self.db = self.client[settings.get('MONGODB_DATABASE')]
        self .collection = self.db['ip_proxy']

    def get_proxy(self):
        #rexExp = re.compile(r'100', re.IGNORECASE)
        result = self.collection.find({"protocol":2,"channel" : 5,'status':1,'continue_failed':0},{'ip':1,'port':1,'success_rate':1})
        if result:
            result = list(result)
        proxy = random.choice(result)
        if proxy:
            proxy = 'https://'+str(proxy['ip'])+':'+str(proxy['port'])
            return proxy

    def find_one_proxy(self, channel):
        success_rate = 100
        # 'success_rate': {'$gt': success_rate}
        proxy_list = []
        while True:
            proxy = {'channel': channel,'success_rate': {'$gt': success_rate},'status': 1,'success': {'$gt': 3}, 'continue_failed': 0}
            cursor = self.collection.find(proxy)
            for p in cursor:
                proxy_list.append(p)
            if len(proxy_list) >= 10:
                p = random.choice(proxy_list)
                return 'http://' + str(p['ip']) + ':' + str(p['port'])
            else:
                success_rate -= 5

