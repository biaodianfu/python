# encoding=utf-8
__author__ = 'lizhipeng'

import datetime
import time
import json
import traceback

import requests

from utils.redisutils import RedisUtil
from utils.load_conf import load_conf
from utils.send_email_utils import server
from utils.model_logger import Logger


class SpiderMonitor(object):

    def __init__(self):
        self.tasks = []
        self.cf = load_conf.load()
        self.r = RedisUtil(self.cf.get('redis', 'redis_host'), self.cf.get('redis', 'redis_port'))
        self.threshold = 0
        self.logger = Logger(self.cf.get('log', 'log_path') + '/spider_monitor')


    def monitor(self):
        while True:
            self.tasks = []
            self.check_task()
            self.logger.info(u'运行的爬虫任务：' + self.tasks.__str__())
            for task in self.tasks:
                # print task
                task_split = task.split(':')
                total = self.check(task_split[0], task_split[1])
                if total is not None:
                    if total <= self.threshold:
                        email_receviers = ['641785844@qq.com','15986389677@139.com']
                        self.logger.info(u'接收警报的邮箱为：' + email_receviers.__str__())
                        msg = task + u'爬虫爬取的数据为：' + str(total) + u'条，请注意！！！！'
                        self.logger.info(msg)
                        server.send_email(email_receviers, msg)
            time.sleep(300)

    def check_task(self):
        try:
            key_list = self.r.check_key('spider_flag_record:*')
            for key in key_list:
                if  'Ota' in key:
                    continue
                if  'ScenicInfo' in key:
                    continue
                if self.r.get(key):
                    self.logger.info(key + u'：' + str(self.r.get(key)))
                    key_split = key.split(':', 1)
                    self.tasks.append(key_split[1])
        except:
            self.logger.error(traceback.format_exc())



    def check(self, spidername, scale):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
                'Host': 'elk.jinxidao.com',
                'Referer': 'http://elk.jinxidao.com/app/kibana',
                'kbn-xsrf-token': '6a985a9931d8f8c67b40dcaa9a5118ca944ce5a658bc1b5687f09217c2d612e4',
                'Authorization': 'Basic YWRtaW46KFljZkFkbWluKQ=='
            }
            url = 'http://elk.jinxidao.com/elasticsearch/_msearch?timeout=0&ignore_unavailable=true&preference=1472032165487'
            check_time_intervals = 5
            now_time = datetime.datetime.now()
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
            past_time = datetime.datetime.now() - datetime.timedelta(minutes=check_time_intervals)
            past_time_date = past_time.date()
            past_time_time_unix = int(time.mktime(past_time.timetuple()) * 1000)
            now_time_unix = int(time.mktime(now_time.timetuple()) * 1000)
            if not past_time_date.__eq__(yesterday):
                data = '{"index":["spider_data_succeed_id-' + past_time_date.strftime(
                    '%Y.%m.%d') + '"],"search_type":"count","' \
                                  'ignore_unavailable":true}\n{"size":0,"query":{"filtered":{"query":{"query_string":{"query":"*' + spidername + '*  ' \
                                  'AND message:' + scale +'","analyze_wildcard":true}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":' + str(
                                    past_time_time_unix) + ',"lte":' + str(
                                  now_time_unix) + ',"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{}}\n'
            else:
                data = '{"index":["spider_data_succeed_id-' + past_time_date.strftime(
                    '%Y.%m.%d') + '", "spider_data_succeed_id-' + now_time.strftime(
                    '%Y.%m.%d') + '"],"search_type":"count","' \
                                  'ignore_unavailable":true}\n{"size":0,"query":{"filtered":{"query":{"query_string":{"query":"*' + spidername + '* ' \
                                 'AND message:' + scale + '","analyze_wildcard":true}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":' + str(
                                  past_time_time_unix) + ',"lte":' + str(
                                  now_time_unix) + ',"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{}}\n'
            r = requests.post(url, data, headers=headers).content
            r_json = json.loads(r)
            total = r_json['responses'][0]['hits']['total']
            if total == 0:
                self.logger.info(data)
            # print r_json['responses'][0]['hits']['total']
            return total
        except:
            self.logger.error(traceback.format_exc())
            return None


m = SpiderMonitor()
m.monitor()
