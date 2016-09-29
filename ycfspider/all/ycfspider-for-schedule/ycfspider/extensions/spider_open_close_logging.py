from scrapy import signals
from scrapy.exceptions import NotConfigured
from ycfspider.utils.spider_state_flag_record import spiderStateRecord
from scrapy.conf import  settings

class SpiderOpenCloseLogging(object):

    def __init__(self):
        # self.item_count = item_count

        self.items_scraped = 0

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise

        # NotConfigured otherwise

        if not crawler.settings.getbool('MYEXT_ENABLED'):

            raise NotConfigured

        # get the number of items from settings

        # item_count = crawler.settings.getint('MYEXT_ITEMCOUNT', 1000)

        # instantiate the extension object

        ext = cls()

        # connect the extension object to signals

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)

        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        # crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        # return the extension object

        return ext

    def spider_opened(self, spider):
        flag_key = spider.name + ':' + settings.get('SCALE')
        spiderStateRecord.flag_record(flag_key)

    def spider_closed(self, spider):
        flag_key = spider.name + ':' + settings.get('SCALE')
        spiderStateRecord.flag_remove(flag_key)

    def spider_idle(self, spider):
        flag_key = spider.name + ':' + settings.get('SCALE')
        spiderStateRecord.flag_remove(flag_key)

    # def item_scraped(self, item, spider):
    #     self.items_scraped += 1
    #     if self.items_scraped % self.item_count == 0:
    #         spider.log("scraped %d items" % self.items_scraped)