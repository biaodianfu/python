# coding=utf-8
import sys
import os
reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from ycfspider.items import YcfspiderItem
import re
import json
import time, datetime
from scrapy.conf import settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.model_logger import Logger
import traceback
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.enum import *
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord

class QunarPcHotelCommentsSpider(RedisSpider):
    name = 'QunarPcHotelCommentsSpider'
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    # 减慢爬取速度 为 2s
    allowed_domains = ["touch.qunar.com", "hotel.qunar.com"]
    start_urls = []
    hotelids = []
    handle_httpstatus_list = [302, 403, 400, 401, 404, 500, 501, 502, 504]
    # 设置每个最大翻页数，测试用
    # max_page = 10
    dict_hotel = {}
    page_num = 500
    # 记录城市列表翻页数
    city_hotel_num = {}
    # pipLine = TutorialPipeline()
    # pc版请求头
    cookie = 'hhs=xiamen_2235%2Cxiamen_2083%2Cdongguan_2923%2Cxiamen_11275%2Cdongguan_4272%2Cdali_6198; QN1=O5cLNVdg0Cg3Vu5Qa027Ag==; QN48=tc_a2671ef6b9b9e3b3_15552599ca5_a409; QN235=2016-06-15; QN99=5045; QunarGlobal=192.168.31.105_-346e0ada_15552c0dc67_2369|1465974018163; QN269=D1A1E3E032C611E6A7E4AC162DBBDDAC; pgv_pvi=7452278784; __ag_cm_=1465974022127; QN57=14659887306190.4666807784085578; QN171="weiyi,A100057881cc0000"; PHPSESSID=3emai67tf4o0k46eumkfmb7k30; QN70=0a61d8d1915570a61048; pgv_si=s2905537536; _jzqckmp=1; QN66=3w; QN5=hotel_wrapper; QN205=cityads6%23mkcdp; __ads_session=Oo0jFPizvQgrdqcCogA=; QN58=1466505189712%7C1466506119903%7C6; QN163=0; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3=1465988721,1466390125,1466404770,1466472996; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1466506870; RT=s=1466506872404&r=http%3A%2F%2Fwww.qunar.com%2F; __utmt=1; QN73=2456-2457; ls=%u4E1C%u839E; flowidList=.2-3.3-1.4-1.1-3.; _jzqx=1.1465974034.1466506877.19.jzqsr=hotel%2Equnar%2Ecom|jzqct=/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/dongguan/; QN25=86b06637-3e35-452c-af21-a92bf245cf96-9f992f90; QN271=1a29d399-a65c-4357-8a2d-0f19f2aa890e; QN43=2; QN42=wzzy1784; _q=U.pdacywi7180; _t=24441785; csrfToken=rqo4XDAuqfoirdldXNiPdITQNHAPauXa; _s=s_TE67A4L54L3SHRO3YUMYTYOH4Y; _v=pc_ciScg2-V_alNenrSJCpLRoBmSmxR-ZiunTAwJRlUZAWNCvPHVE5eZuq73HLeY0mNvfjIHdxrenTffR5hGVpI695rvaQ-NvQf3-ZjhThxcFPtaXwvSSJTCzZGGDe6sRpHZjaAhMssUALlBshtbQEUexR9Slz6IKpfbTAIBMO9D; QN267=1466507221345_68f8b45012f79c83; QN44=pdacywi7180; _i=RBTjeL-rKGEx8zowssyLI74pF0-x; _vi=-CBLaT_d9-Tff7P3zyLotp0cKpR6DeNIMhD-3R8zDxTkGfhhP9-ZUpCMDmFlTXlSLZ7q_vY3eG1UplDf9sOQt5plkfHqagIjnlPyevfmMYqwSYMdY-A0VJvVohKwDBpXlkYOd7qBrIgX2e8iEUItLtkQhBL99SqN-spbw0-lNxPv; RT_CACLPRICE=1; ag_fid=6AP5XFdOorVrNgwF; __utma=183398822.547434741.1465974022.1466501680.1466506003.24; __utmb=183398822.7.10.1466506003; __utmc=183398822; __utmz=183398822.1466506003.24.20.utmcsr=hotel.qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/city/dongguan/dt-2923/; _qzja=1.2124178427.1465974034104.1466499171791.1466506876569.1466506895348.1466507223372..0.0.77.21; _qzjb=1.1466506876569.4.0.0.0; _qzjc=1; _qzjto=37.5.0; JSESSIONID=B0D666275882A23B5310C1F6D945B763; _jzqa=1.2330205701161542700.1465974034.1466499172.1466506877.21; _jzqc=1; _jzqb=1.7.10.1466506877.1; QN268=1466507221345_68f8b45012f79c83|1466507223491_f7797bf420fb298b'
    headers = {'cookie': cookie,
               'Accept - Encoding': 'gzip, deflate, sdch',
               "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.82 Safari/537.36"
               }
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:qunar_pc_hotel_comment_all'
    else:
        redis_key = 'spider:qunar_pc_hotel_comment_ycf'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        super(QunarPcHotelCommentsSpider, self).__init__(*args, **kwargs)
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        # spiderStateRecord.flag_record(self.name)
    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        if self.server.llen(self.name + ':requests') == 0:
            item = self.server.lpop(self.redis_key)
            if item:
                try:
                    item = eval(item)
                    hotelid = item["hotel_id"]
                    url = "http://te.review.qunar.com/api/h/" +hotelid + "/detail/rank/v1/page/1?u2=null&__jscallback=jQuery183007184270187281072_1473421957598&rate=all&onlyGuru=false"
                    return  Request(url=url, meta={'item': item,"hotelid":hotelid},headers=self.headers,dont_filter=True,callback=self.parse)
                except Exception, e:
                    error_log_dic = {}
                    error_log_dic['channel_id'] = ChannelEnum.QUNAR
                    error_log_dic['platform_id'] = PlatformEnum.PC
                    error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
                    error_log_dic['id'] = item.get('id', '')
                    error_log_dic['pid'] = ''
                    error_log_dic['error_info'] = traceback.format_exc()
                    error_log_dic['error_type'] = ErrorTypeEnum.LOGICERROR
                    error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    self.logger_error.error(json.dumps(error_log_dic))

    # 解析酒店评论数量
    def parse(self, response):
        try:
            resp_str = response.body
            self.logger_data.info(resp_str)
            body = resp_str[42:len(resp_str) - 2]
            json_r = json.loads(body)
            item = YcfspiderItem()
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_COMMENT_RESOURSE_PATH')
            hotelid = response.meta["hotelid"]
            results = {
                "object_id": hotelid, "object_class_id": 1, "channel_id": ChannelEnum.QUNAR,
                "platform_id": PlatformEnum.PC, "user_name": "", "product_used_time": "",
                "product_property": " ", "travel_type": -1, "book_from": "", "final_score": "", "position_score": "",
                "facility_score": "",
                "service_score": "", "health_score": "", "total_comments": -1, "total_pictures": -1,
                "has_picture": -1,
                "comment_details": "",
                "comment_time": "", "praise_count": -1, "other_user_comment_count": -1, "sort": "",
                "comment_client_id": -1, "is_high_quality_comment": -1, "reserved_col1": "", "comfort_score": "",
                "food_score": "",
                "reserved_col2": "",
                "reserved_col3": "",
                "reserved_col4": "", "reserved_col5": "", "remark": "",
                "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"), "comment_id": "", "parent_comment_id": "",
                "comment_key_words": ""
            }
            #获取评论总数
            total = int(json_r["data"]["count"])
            #页数
            page = total/10
            pageNum = page if total%10==0 else page+1
            currorPage = 0
            for currorPage in range(0,pageNum):
                currorPage += 1
                url = "http://te.review.qunar.com/api/h/" +hotelid + "/detail/rank/v1/page/" + str(
                    currorPage) + \
                      "?u2=null&__jscallback=jQuery183007184270187281072_1473421957598&rate=all&onlyGuru=false"
                yield Request(url=url, meta={'item': item, "results": results}, callback=self.parse_content_assess,
                              dont_filter=True,headers=self.headers)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = item.get('id', '')
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.LOGICERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    def parse_content_assess(self,response):
        try:
            resp_str = response.body
            item = response.meta['item']
            results = response.meta['results']
            body = resp_str[42:len(resp_str) - 2]
            json_r = json.loads(body)
            # 存放评论dict
            list = response.meta.get("list")
            # 存放酒店的一条评论
            content_list = json_r.get("data").get("list")
            for content in content_list:
                commentid= content.get("feedOid")
                results["comment_id"] = commentid
                results["user_name"] = content.get("nickName")
                if content.get("feedType") == 4:
                    results["is_high_quality_comment"] = 1
                else:
                    results["is_high_quality_comment"] = 0

                json_content = json.loads(content.get("content"))
                results["comment_details"] = json_content.get("feedContent")
                results["travel_type"] = json_content.get("tripType")
                results["comment_time"] = json_content.get("modtime")
                results["product_property"] = json_content.get("roomType")
                results["comment_key_words"] = json_content.get("title")

                if json_content.get("imageUrl"):
                    results["total_pictures"] = len(json_content.get("imageUrl"))
                    results["has_picture"] = 1
                else:
                    results["total_pictures"] = 0
                    results["has_picture"] = 0
                results["product_used_time"] = json_content.get("checkInDate")
                results["final_score"] = json_content.get("evaluation")
                results["book_from"] = json_content.get("from")
                sub_scores = json_content.get("subScores")
                if sub_scores:
                    results["position_score"] = sub_scores[3].get("score")
                    results["facility_score"] = sub_scores[0].get("score")
                    results["service_score"] = sub_scores[2].get("score")
                    results["health_score"] = sub_scores[1].get("score")
                url = "http://review.qunar.com/api/comment/stat?__jscallback=jQuery18308208622033707798_1474624666774&cids="+results["comment_id"]
                yield Request(url,callback=self.parse_stat,meta = {"item":item,"commentid":commentid,"results":results},headers=self.headers,dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = item.get('id', '')
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.LOGICERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    #解析点赞数、回复数等
    def parse_stat(self,response):
        resp_str = response.body

        json_body = json.loads(resp_str[41:len(resp_str) -1])
        item = response.meta["item"]
        results = response.meta["results"]
        commentid = response.meta["commentid"]
        cont = json_body[commentid]
        results["praise_count"] = cont["likeCount"]
        results["other_user_comment_count"] = cont["replyCount"]
        item["results"] = results
        # print item
        yield item
        # pass