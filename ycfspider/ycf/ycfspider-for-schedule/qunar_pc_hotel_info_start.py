import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import sys
import time
from scrapy.cmdline import execute

class QunarPcHotelInfoStart():
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
        log_path = self.conf['spider_log_path']+'/QunarPcHotelInfoSpider/spider_log_'+str(int(time.time()))
        sys.argv = ["scrapy", "crawl", "QunarPcHotelInfoSpider", "--logfile="+log_path]
        # sys.argv = ["scrapy", "crawl", "QunarPcHotelInfoSpider"]
        execute()


if __name__ == '__main__':
    start = QunarPcHotelInfoStart()
    start.run()
