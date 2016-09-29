# -*- coding: utf-8 -*-

BOT_NAME = 'ycfspider'

SPIDER_MODULES = ['ycfspider.spiders']
NEWSPIDER_MODULE = 'ycfspider.spiders'
TELNETCONSOLE_ENABLED=True

LOG_LEVEL = 'INFO'
CONCURRENT_REQUESTS = 10

# REDIRECT_ENABLED = False
DOWNLOAD_DELAY=0.5
DOWNLOAD_TIMEOUT = 15
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN= 100
CONCURRENT_REQUESTS_PER_IP= 100

MYEXT_ENABLED=True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [302, 500, 503, 504, 400, 403, 404, 408]
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'ycfspider.middleware.proxy_middleware.ProxyMiddleware': 10,
    'ycfspider.middleware.hotel_info_middlewares.HotelInfoMiddleware': 20,
    'ycfspider.middleware.ota_middleware.OtaMiddleware': 25,
    'ycfspider.middleware.qunar_pc_hotel_price_requests_middleware.QunarPcHotelPriceRequestsMiddleware': 30,
    'ycfspider.middleware.hp_check_crawl_data_middleware.HpCheckCrawlDataMiddleWare': 40,
    'ycfspider.middleware.sp_check_crawl_data_middleware.SpCheckCrawlDataMiddleWare': 50,
    'ycfspider.middleware.ctrip_m_hotel_price_keys_middleware.CtripMHotelPriceKeysMiddleware': 601

}

ITEM_PIPELINES = {
        'ycfspider.pipelines.kafka_pipelines.KafkaPipeline':20,
        'ycfspider.pipelines.log_pipelines.LogPipelines': 10
}

EXTENSIONS = {
    'ycfspider.extensions.spider_open_close_logging.SpiderOpenCloseLogging': 500,
}

DNSCACHE_ENABLED = True

#mongo配置
MONGODB_HOST = 'mongodb01.ycf.com'
MONGODB_PORT = 27200
MONGODB_DATABASE = 'hotelspiders'

#scrapy-redis config
REDIS_HOST = 'spider-redis.ycf.com'
REDIS_PORT = 6379
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderQueue"

QUNAR_CRAWL_DATE = 7
ELONG_CRAWL_DATE = 7
CTRIP_CRAWL_DATE = 7

#proxy config
PROXY_ADDRESS = 'http://spider-proxy.ycf.com:8080'
# log path config
# LOG_FILE = '/local/crawler/data/elong/spider_p1.log'
LOG_PATH = '/data'

#KAFKA配置
KAFKA_ADDRESS = 'http://kafka.ycf.com:81'
KAFKA_HOTEL_PRICE_RESOURSE_PATH = '/lac/cp/room/product/price'
KAFKA_HOTEL_INFO_RESOURSE_PATH = '/lac/cp/channel/hotels'
KAFKA_SCENIC_PRICE_RESOURSE_PATH = '/lac/cp/scenic/product/price'
KAFKA_SCENIC_INFO_RESOURSE_PATH = '/lac/cp/channel/scenic'
KAFKA_OTA_HOTEL_PRICE_RESOURSE_PATH = '/lac/ota/room/product/price'
KAFKA_OTA_HOTEL_INFO_RESOURSE_PATH = '/lac/ota/hotels'
KAFKA_OTA_HOTEL_STOCK_RESOURSE_PATH = '/lac/ota/hotel/stock'

#抓取规模配置 all:全量 ycf：要出发在售酒景
SCALE = 'ycf'
# 当抓取为ycf时，这个字段才有效
REDIS_DB = 1

#是否保留仍未爬取的requests
PERSIST_REQUESTS = True

#对于一些网站，可以通过此参数抓取ajax信息，通过HTML.默认为False
AJAXCRAWL_ENABLED = True

LOOP=True

# #定义连接mysql数据库中的用户名和密码表
# LOGIN_MYSQL_HOST = '192.168.9.224'
# LOGIN_MYSQL_PORT = 3306
# LOGIN_MYSQL_USERNAME = 'root'
# LOGIN_MYSQL_USERPWD = 'ycf_test'
# LOGIN_MYSQL_DB = 'ycf_priceratio'
# LOGIN_MYSQL_TABLE = 'channel_substation'

#验证码破解地址
YZM_ADDRESS = 'http://106.75.143.158:8080'
