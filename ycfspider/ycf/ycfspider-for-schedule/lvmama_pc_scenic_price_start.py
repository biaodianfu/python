__author__ = 'lizhipeng'

from scrapy.cmdline import execute
import sys
import time
class LvmamaPcScenicPriceStart():

    def __init__(self):
        self.conf = {}
        self.load_conf()

    def load_conf(self):
        with open('conf/spider_start.conf') as f:
            conf_str = f.readlines()
        for item in conf_str:
            item_spilt = item.split('=')
            self.conf[item_spilt[0].rstrip()] = item_spilt[1].rstrip('\n').rstrip()
    def run(self):
        log_path = self.conf['spider_log_path']+'/LvmamaPcScenicPriceSpider/spider_log_'+str(int(time.time()))
        sys.argv = ["scrapy", "crawl", "LvmamaPcScenicPriceSpider", "--logfile="+log_path]
        # sys.argv = ["scrapy", "crawl", "LvmamaPcScenicPriceSpider"]
        execute()
#
# sys.argv = ["scrapy", "crawl", "elong_spider_slave"]
# execute()

if __name__=='__main__':
    start = LvmamaPcScenicPriceStart()
    start.run()