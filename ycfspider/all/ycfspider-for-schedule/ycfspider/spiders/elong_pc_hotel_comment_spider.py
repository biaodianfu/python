# coding=utf-8
import sys
import os

from ycfspider.utils.enum import *
from ycfspider.utils.redisutils import RedisUtil

reload(sys)
sys.setdefaultencoding("utf-8")
from ycfspider.items import YcfspiderItem
import json, re
from scrapy.http import Request
from bs4 import BeautifulSoup
import datetime, time
from scrapy_redis.spiders import RedisSpider
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.model_logger import Logger
from scrapy.conf import settings
import traceback
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord


class ElongPcHotelCommentSpider(RedisSpider):
    name = "ElongPcHotelCommentSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["hotel.elong.com"]
    start_urls = []
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:elong_pc_hotel_comment_all'
    else:
        redis_key = 'spider:elong_pc_hotel_comment_ycf'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        super(ElongPcHotelCommentSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        if self.server.llen(self.name + ':requests') == 0:
            item = self.server.lpop(self.redis_key)
            if item:
                # print item
                item = eval(item)
                urlstr = 'http://hotel.elong.com/' + item["hotel_id"] + '/'
                return Request(urlstr, meta={'cityId': item["city_id"], 'hotelname': item["hotel_name"],
                                             'lat': item["lat"], 'lng': item["lng"],
                                             'hoteladdress': item["hotel_address"], 'cityNameEn': item["city_name_gb"],
                                             'hotelid': item["hotel_id"]}, dont_filter=True)

    def parse(self, response):
        try:
            item = YcfspiderItem()
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_COMMENT_RESOURSE_PATH')
            hotelid = response.meta["hotelid"]
            results = {
                "object_id": hotelid, "object_class_id": 1, "channel_id": ChannelEnum.ELONG,
                "platform_id": PlatformEnum.PC, "user_name": "", "product_used_time": "",
                "product_property": " ", "travel_type": -1, "book_from": "", "final_score": "", "position_score": "",
                "facility_score": "",
                "service_score": "", "health_score": "", "total_comments": -1, "total_pictures": -1,
                "has_picture": -1,
                "comment_details": "",
                "comment_time": "", "praise_count": -1, "other_user_comment_count": -1, "sort": "",
                "comment_client_id": -1, "is_high_quality_comment": -1, "reserved_col1": "","comfort_score":"","food_score":"",
                "reserved_col2": "",
                "reserved_col3": "",
                "reserved_col4": "", "reserved_col5": "", "remark": "",
                "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),"comment_id":"","parent_comment_id":"","comment_key_words":""
            }

            # 先获取评论总数
            hotelStr = response.body
            self.logger_data.info(hotelStr)
            soup = BeautifulSoup(hotelStr, "lxml")  # 开始解析
            hotel_comment_msg = soup.find(attrs={"class": "hrela_comt bbddd hrela_comt1"})

            if hotel_comment_msg.find(attrs={"class": "hrela_comt_total"}) is not None:
                parsecomment_count = soup.find(attrs={"class": "hrela_comt_total"}).find("a")
                if parsecomment_count is not None:
                    total_comment_count =  parsecomment_count.text
                    # 评论总数转数字
                    mod = re.compile(r'\d+')
                    total_comment_count = int(mod.findall(total_comment_count)[0])
                    page = total_comment_count/20
                    totalPage = page if total_comment_count%20==0 else page+1
            else :
                total_comment_count = 0
                totalPage = 0
            currorPage = 0
            for currorPage in range(0,totalPage):
                currorPage += 1
                url = 'http://hotel.elong.com/ajax/detail/gethotelreviews?hotelId=' + hotelid + \
                      '&recommendedType=0&pageIndex=' + str(currorPage) + '&roomTypeId=0&mainTagId=0&subTagId=0'
                yield Request(url=url, meta={'item': item,"results":results},
                          callback=self.parse_comment_content, dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['results']["hotel_id"]
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_comment_content(self,response):
        try:
            body = json.loads(response.body)
            self.logger_data.info(body)
            results = response.meta['results']
            item = response.meta['item']
            contents = body.get("contents")
            for content in contents:
                results['comment_id'] = content.get("commentId")
                results['comment_details'] = content.get("content")
                results['user_name'] = content.get("commentUser").get("nickName")
                results["total_comments"] = content.get("commentUser").get("commentCount")
                results['comment_time'] = content.get("createTimeString")
                commentExt = content.get("commentExt")
                results['product_property'] = commentExt.get("order").get("roomTypeName")

                # 使用时间戳
                str1 = commentExt.get("order").get("checkInTime") / 1000
                results['product_used_time'] = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(int(str1)))
                travel_type = commentExt.get("travelType")
                if travel_type == 2:
                    results["travel_type"] = "情侣"
                elif travel_type == 4:
                    results["travel_type"] = "商务出差"
                elif travel_type == 0:
                    results["travel_type"] = "其他"
                elif travel_type == 1:
                    results["travel_type"] = "带小孩"
                elif travel_type == 5:
                    results["travel_type"] = "独自旅行"
                elif travel_type == 3:
                    results["travel_type"] = "团体出行"
                # commentExt.get("order").get("roomTypeId")
                if content.get("images"):
                    results["has_picture"] = 1
                    results["total_pictures"] = len(content.get("images"))
                else:
                    results["has_picture"] = 0
                    results["total_pictures"] = 0
                if content.get("replys"):
                    results["other_user_comment_count"] = len(content.get("replys"))
                else:
                    results["other_user_comment_count"] = 0

                results["is_high_quality_comment"] = 1 if content.get("marrow")  else 0
                # 评分，是否推荐
                recomend = content.get("recomend")
                if recomend == 1:
                    score = 5  # "值得推荐"
                elif recomend == 2:
                    score = 0  # "有待改善"
                    results["final_score"] = score

                source = content.get("source")
                if source == 2:
                    results["book_from"] = "手机网页"
                elif source == 1:
                    results["book_from"] = "手机app"
                else:
                    results["book_from"] = "pc"
                item['results'] = results
                # print item
                yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.ELONG
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['results']["hotel_id"]
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    # def close(spider, reason):
    #     spiderStateRecord.flag_remove(spider.name)
    #     closed = getattr(spider, 'closed', None)
    #     if callable(closed):
    #         return closed(reason)