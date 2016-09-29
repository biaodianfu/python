# encoding=utf-8
import json
import traceback
import datetime
import sys
import copy
import os

from scrapy_redis.spiders import RedisSpider

from scrapy.http import Request
from scrapy.conf import settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.items import YcfspiderItem
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.tables.scenic_info_table import scenic_info_table
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
    'Host': 'lvyou.meituan.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'userid': -1,
    'User-Agent': 'AiMeiTuan /HONOR-5.1.1-KIW-TL00H-1776x1080-480-7.0.1-401-860707030258106-huawei1',
    'Cookie': 'JSESSIONID=xlq2iv5isu6t1ptkj2y2rqagv'
}
headers2 = {
    'Host': 'api.meituan.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'userid': -1,
    'User-Agent': 'AiMeiTuan /HONOR-5.1.1-KIW-TL00H-1776x1080-480-7.0.1-401-860707030258106-huawei1',
    'Cookie': 'JSESSIONID=xlq2iv5isu6t1ptkj2y2rqagv'
}

class MeituanAppScenicInfoSpider(RedisSpider):
    name = 'MeituanAppScenicInfoSpider'

    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:meituan_app_scenic_info_all'
    else:
        redis_key = 'spider:meituan_app_scenic_info_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(MeituanAppScenicInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)


    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            return item

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def schedule_next_request(self):
        item = self.next_request()
        if item:
            item = eval(item)
            url = 'http://lvyou.meituan.com/volga/api/v1/trip/poi/' + str(
                    item["scenic_id"]) + '?fields=phone,scoreSource,markNumbers,cityId,addr,lng,' \
                                         'hasGroup,subwayStationId,cates,frontImg,chooseSitting,wifi,avgPrice,style,featureMenus,avgScore,name,parkingInfo,lat,cateId,introduction,' \
                                         'showType,areaId,districtId,preferent,lowestPrice,cateName,areaName,zlSourceType,campaignTag,mallName,mallId,brandId,brandName,brandLogo,brandStory,' \
                                         'smPromotion,ktv,geo,historyCouponCount,recommendation,iUrl,isQueuing,newPayInfo,payInfo,sourceType,abstracts,groupInfo,isSuperVoucher,discount,isWaimai,' \
                                         'collectionDeals,nearPoi,sparkle,topicList,cityIds,showChannel,showStatus,tour,commentModel,cityName,desc&utm_source=wandoujia&utm_medium=android&utm_term=411' \
                                         '&version_name=7.1.1&utm_content=863396020214969&utm_campaign=AgroupBgroupC0E1585400505716309504_e73cce65173d16088606f05965653db99_c3_dtrippoipolyb_mnearbyptag2' \
                                         'Ghomepage_category8_296__a1&ci=10&msid=8633960202149691470290933551&uuid=78450BCEDEF11CA56C5E3E136B469A692A0B6E9E0F10C23B23CC55A0C32978CF&userid=-1'
            req = Request(url, meta={"item": item}, headers=headers, dont_filter=True, errback=self.download_errback,
                          callback=self.parse)
            self.crawler.engine.crawl(req, spider=self)

    def parse(self, response):
        try:
            body = json.loads(response.body)
            #print type(body)
            self.logger_data.info(body)
            data = response.meta["item"]
            results = copy.deepcopy(scenic_info_table)
            results['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            results['scenic_id'] = data["scenic_id"]
            # results['city_name_cn'] = data["city_name_cn"]
            # results['city_name_gb'] = data["city_name_en"]
            results['currency'] = 1
            results['platform_id'] = 1
            results['channel_id'] = 4
            results['scenic_belongs'] = 0

            picture_list_url = []
            if 'data' in body.keys():
                data_info_0 = {}
                data_info = body["data"]
                if data_info:
                    data_info_0 = data_info[0]
                data_info_keys = data_info_0.keys()
                if 'avgScore' in data_info_keys:
                    results["score"] = data_info_0["avgScore"]
                if 'addr' in data_info_keys:
                    results["address"] = data_info_0["addr"]
                if 'name' in data_info_keys:
                    results["scenic_name"] = data_info_0["name"]
                if 'phone' in data_info_keys:
                    results["telephone"] = data_info_0["phone"]
                if 'frontImg' in data_info_keys:
                    results["picture_url"] = data_info_0["frontImg"]
                    picture_list_url.append(results["picture_url"])
                if 'introduction' in data_info_keys:
                    results["desc"] = results["desc"] + data_info_0["introduction"] + '##'
                if 'lat' in data_info_keys and 'lng' in data_info_keys:
                    results["longitude"] = data_info_0["lng"]
                    results["latitude"] = data_info_0["lat"]
                if 'wifi' in data_info_keys:
                    if data_info_0['wifi']:
                        results['has_wifi'] = 1
                    else:
                        results['has_wifi'] = 0
                if 'commentModel' in data_info_keys:
                    commentModel = data_info_0["commentModel"]
                    if 'total_comment' in commentModel.keys():
                        results["total_comment_count"] = commentModel["total_comment"]
                    if 'feedback' in commentModel.keys():
                        feedback_0 = {}
                        feedback = commentModel["feedback"]
                        if feedback:
                            feedback_0 = feedback[0]
                        if 'picInfo' in feedback_0.keys():
                            picInfo = feedback_0["picInfo"]
                            for picurl in picInfo:
                                picture_list_url.append(picurl["url"])
                if 'tour' in data_info_keys:
                    tour = data_info_0["tour"]
                    tour_keys = tour.keys()
                    if 'tourPlaceStar' in tour_keys:
                        results["level"] = tour["tourPlaceStar"]
                    if 'tourOpenTime' in tour_keys:
                        results["opening_hours"] = tour["tourOpenTime"]
                    if 'tourDetailDesc' in tour_keys:
                        results["desc"] = results["desc"] + tour["tourDetailDesc"] + '##'
                if 'avgPrice' in data_info_keys:
                    results["ticket_price"] = data_info_0["avgPrice"]
                if 'lowestPrice' in data_info_keys:
                    results["min_price"] = data_info_0["lowestPrice"]
                if 'cateName' in data_info_keys:
                    results["type"] = data_info_0["cateName"]
                if 'areaName' in data_info_keys:
                    results["district_name"] = data_info_0["areaName"]
                if 'cityName' in data_info_keys:
                    # print '+++++++++++++++++'
                    # print data_info_0["cityName"]
                    results["city_name_cn"] = data_info_0["cityName"]
                if 'areaId' in data_info_keys:
                    results["district_code"] = data_info_0["areaId"]
                if picture_list_url:
                    results["picture_list_url"] = picture_list_url
                url = 'http://lvyou.meituan.com/volga/api/v2/trip/poi/info/desc?poiId=' + str(
                        data["scenic_id"]) + '&utm_source=wandoujia&utm_medium=android&utm_term=411&version_name=7.1.1' \
                                             '&utm_content=863396020214969&utm_campaign=AgroupBgroupC0E1585400505716309504_e5cc9ccd64fd0a146143c1244cb077a1a_c0_dtrippoipolyb_mnearbyptag2Ghomepage_category' \
                                             '8_296__a1&ci=10&msid=8633960202149691470358214385&uuid=78450BCEDEF11CA56C5E3E136B469A692A0B6E9E0F10C23B23CC55A0C32978CF&userid=-1'
                yield Request(url, meta={"results": results}, callback=self.parse_info, dont_filter=True, headers=headers)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta['item']['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    def parse_info(self, response):
        try:
            results = response.meta["results"]
            body = json.loads(response.body)
            self.logger_data.info(body)
            tips = ''
            if 'data' in body.keys():
                data = body["data"]
                data_keys = data.keys()
                if 'advicePlayHour' in data_keys:
                    tips = tips + '建议游玩时长：' + data["advicePlayHour"] + ';'
                if 'bestPlayTime' in data_keys:
                    tips = tips + '最佳游玩时间：' + data["bestPlayTime"]
                results["tips"] = tips
                if 'trafficDetail' in data_keys:
                    results["surround_bus_station"] = data["trafficDetail"]
                if 'introDetail' in data_keys:
                    describe = ''
                    introDetail = data["introDetail"]
                    for descdata in introDetail:
                        if 'type' in descdata.keys():
                            if descdata['type'] == 'img':
                                if 'content' in descdata.keys():
                                    results['picture_list_url'].append(descdata["content"])
                            if descdata['type'] == 'text':
                                if 'content' in descdata.keys():
                                    describe = describe + descdata["content"] + '##'
                    results["desc"] = results["desc"] + describe

            url = 'http://api.meituan.com/group/v1/poi/' + str(
                    results["scenic_id"]) + '/imgs?classified=true&mpt_poiid=' + str(results["scenic_id"]) + \
                  '&__vhost=api.mobile.meituan.com&utm_source=wandoujia&utm_medium=android&utm_term=411&version_name=7.1.1&utm_content=863396' \
                  '020214969&utm_campaign=AgroupBgroupC0E1585400505716309504_ef134886343d11afdd5d5db37697172fa_c0_dtrippoipolyb_mnearbyptag2Ghom' \
                  'epage_category8_296__a1&ci=20&msid=8633960202149691470378236243&uuid=78450BCEDEF11CA56C5E3E136B469A692A0B6E9E0F10C23B23CC55A0' \
                  'C32978CF&userid=-1'
            yield Request(url, meta={"results": results}, callback=self.parse_pic, dont_filter=True, headers=headers2)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_pic(self, response):
        try:
            item = YcfspiderItem()
            results = response.meta["results"]
            body = json.loads(response.body)
            self.logger_data.info(json.dumps(body))
            if 'data' in body.keys():
                data = body["data"]
                if data:
                    for imgsdict in data:
                        if 'imgs' in imgsdict.keys():
                            imgs = imgsdict["imgs"]
                            for img in imgs:
                                if 'urls' in img.keys():
                                    if img["urls"]:
                                        for url in img["urls"]:
                                            results["picture_list_url"].append(url)
            item["results"] = results
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_INFO_RESOURSE_PATH')
            #print item
            yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.MEITUAN
            error_log_dic['platform_id'] = PlatformEnum.APP
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def download_errback(self, e):
        print type(e), repr(e)

    # def close(spider, reason):
    #         spiderStateRecord.flag_remove(spider.name)
    #         closed = getattr(spider, 'closed', None)
    #         if callable(closed):
    #             return closed(reason)