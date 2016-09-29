# encoding=utf-8
from scrapy.conf import settings
from redisutils import RedisUtil

__author__ = 'lizhipeng'


class SpiderStateRecord(object):
    def __init__(self):
        self.r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)

    def flag_record(self, spidername):
        key = 'spider_flag_record:' + spidername
        self.r.incr(key)

    def flag_remove(self, spidername):
        key = 'spider_flag_record:' + spidername
        self.r.decr(key)

spiderStateRecord = SpiderStateRecord()
# spiderStateRecord.flag_remove('test')
