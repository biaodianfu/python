#encoding=utf-8
from pymongo import MongoClient
from pymongo.collection import Collection

__author__ = 'lizhipeng'

class WriteCookie(object):

    def __init__(self):
        self.collection = ''
        self.r = ''
        self.conf = {}
        self.client = ''

    def run(self):
        self.load_conf()
        self.mongo_connect()
        self.fill_cookie()

    def load_conf(self):
        with open('conf/master.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()

    def mongo_connect(self):
        self.client = MongoClient(self.conf['mongo_host'], int(self.conf['mongo_port']))
        db = self.client[self.conf['mongo_db']]
        self.collection = Collection(db, 'qunar_cookie')

    def fill_cookie(self):
        self.collection.remove()
        with open('cookies', 'r') as f:
            cookie_str = f.read().replace('\n', '')
        cookie_list = cookie_str.split('##')
        for item in cookie_list:
            write_data = {}
            write_data['cookie'] = item.replace('\r', '')
            self.collection.insert(write_data)

if __name__=='__main__':
    w = WriteCookie()
    w.run()
