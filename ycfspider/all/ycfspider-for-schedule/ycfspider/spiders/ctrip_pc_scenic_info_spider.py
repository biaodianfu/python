# -*- coding: utf-8 -*-
import datetime
import json
import copy
import traceback
import os
import time

from scrapy_redis.spiders import RedisSpider

from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from scrapy.http import Request
from scrapy.selector import Selector
from ycfspider.items import YcfspiderItem
from ycfspider.utils.webutil import WebUtil
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.tables.scenic_info_table import scenic_info_table
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from scrapy.exceptions import DontCloseSpider


class CtripPcScenicInfoSpider(RedisSpider):
    name = "CtripPcScenicInfoSpider"
    allowed_domains = ["piao.ctrip.com"]
    start_urls = []
    #设置每个最大翻页数，测试用
    dict_scenic = []
    page_num = 500

    #记录城市列表翻页数
    city_hotel_num = {}
    #pc版请求头
    cookie = 'QN1=eIQiQleXUScORgs1ErNtAg==; QN269=55C125A052F711E69E4E6C3BE5A8881C; pgv_pvi=1293347840; QN99=312; QN48=tc_39e92ba09a2996f2_1562fe2cab8_ce4c; QN73=2493-2494; __utma=183398822.417211823.1469534507.1469763366.1469779631.7; __utmz=183398822.1469779631.7.7.utmcsr=hotel.qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/city/suzhou_jiangsu/dt-10664; csrfToken=G1TpIdkE3vrAmoD23N8JxgpTnPkHs1ZV; QunarGlobal=10.86.213.138_435e4276_15649d12280_1639|1470119069499; QN163=0; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3=1470119070; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1470119070; PHPSESSID=244iqgakdpq2gq13gd34nc5en2; QN268=|1470119070426_0aa07af9e443058a; QN71="MTIxLjMzLjIxMC4xNjI65bm/5LicOjE="; QN57=14701190746640.009358836753765587; _i=RBTKAD-rKGEx-oLTsUaIwTcNODmx; _vi=j6kcMnxJEiBbthvtQTPRCHDoKcZQLVefd8nnMQ_nuOFsJ3VylRvAfh_hwj1vlguHzZMbfyA13ncVDuGR5fhMGkBX3IsOUffdb-edDPGlgUcD-ONKYdg82C9HXF9z7mxvjYfSs2r9UAlOpSB8xh8i_I9Rq1nGcZ1ddh9R1eLPMgcs; QN58=1470119074663%7C1470120266930%7C7; JSESSIONID=59307D4D7775E5675D1C7003D4ECCA99'
    headers = {'cookie': cookie,
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate, sdch',
               'Accept-Language': 'zh-CN,zh;q=0.8',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'piao.ctrip.com',
               'Upgrade-Insecure-Requests': 1
               }

    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:ctrip_pc_scenic_info_all'
    else:
        redis_key = 'spider:ctrip_pc_scenic_info_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(CtripPcScenicInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider


    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            url = 'http://piao.ctrip.com/dest/'+str(item["scenic_id"])+'.html'
            return Request(url,meta={'item':item},headers=self.headers,dont_filter=True)

    def parse(self, response):
        try:
            sel = Selector(response)
            item = YcfspiderItem()
            body = response.body
            self.logger_data.info(body)
            data = response.meta['item']
            item["kafka_url"] = settings.get("KAFKA_ADDRESS") + settings.get("KAFKA_SCENIC_INFO_RESOURSE_PATH")
            # results = {"id":"","scenic_id":data["scenic_id"],"scenic_name":data["scenic_name"],"scenic_used_name":"","platform_id":1,"channel_id":1,"scenic_belongs":0,"group_belongs":"","country_code":"CN","country_name":"china","province_code":"","province_name":data['scenic_province'],"city_code":"","city_name_cn":"","city_name_gb":"","district_code":"","district_name":"","short_address":"",
            #            "address":data["scenic_address"],"postal_code":"","position_type":u"经纬度","longitude":"","latitude":"","telephone":"","email":"","fax":"","url":"","picture_url":"","picture_list_url":"","desc":"","scenic_ad":"","scenic_notice":"","type":"","level":data['scenic_level'],"score":data['scenic_score'],"currency":"","ticket_price":"","min_price":"","tips":"","comment_desc":"",
            #            "total_comment_count":"","good_comment_rate":"","favourite_count":"","praise_count":"","share_count":"","opening_hours":"","policy":"","is_credit_card_supportted":"","facility":"","scenic_service":"","has_parking_lot":"","has_wifi":"","has_atm":"","has_restaurant":"","has_gym":"","shared_url":"","active_facility":"",
            #            "surround_airport":"","surround_metro":"","surround_railway_station":"","surround_bus_station":"","surround_rent_car":"","surround_scenic":"","surround_hotel":"","surround_shopping":"","surround_food":"","surround_entertainment":"","status":"","other_welfare":"","remark":"","keywords":"","reserved_col1":"","reserved_col2":"","reserved_col3":"","reserved_col4":"",
            #            "reserved_col5":"","crawl_version":"","crawl_time":time.strftime("%Y-%m-%d %H:%M:%S")}
            results = copy.deepcopy(scenic_info_table)
            results['scenic_id'] = data["scenic_id"]
            results['platform_id'] = 1
            results['channel_id'] = 1
            results['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
             # 景区名称
            scenic_name_xpath ="//div[@class='media-right']/h2[@class='media-title']/text()"
            scenic_name = sel.xpath(scenic_name_xpath).extract()
            if scenic_name:
                results["scenic_name"] = scenic_name[0]
            # print "景区名称：",results["scenic_name"]
            # 景区地址
            address_xpath = "//div[@class='media-right']/ul/li/span/text()"
            address = sel.xpath(address_xpath).extract()
            if address:
                results["address"] = address[0].strip()
            # print "景区地址：",results["address"]
            # 景区省份 景区城市
            main_nav_xpath = "//div[@id='main-nav']/a/text()"
            main_nav = sel.xpath(main_nav_xpath).extract()
            if main_nav_xpath:
                results["province_name"] = main_nav[1][:-5]
                results["city_name_cn"] = main_nav[2][:-5]
            # print "省份：",results["province_name"]
            # print "城市中文名称：",results["city_name_cn"]
            # 景区等级
            level_xpath = "//div[@class='media-right']/span[@class='media-grade']/strong/text()"
            level = sel.xpath(level_xpath).extract()
            if level:
                results["level"] = level[0] + "景区"
            # print "景区等级：",results["level"]
            # 景区评分
            score_xpath = '//div[@class="grade"]/i/text()'
            score = sel.xpath(score_xpath).extract()
            if len(score) > 0:
                for string in score:
                    results["score"] = string + u"分/5分"
            # print u"" \
            #       u"评分：", results['score']
            #交通信息
            surround_traffic_xpath = '//div[@class="feature-traffic"]'
            results["surround_bus_station"] = WebUtil.get_web_static_info(response,surround_traffic_xpath)
           # print '交通信息：',results["surround_bus_station"]
            #景区描述
            desc_xpath = '//div[@class="feature-content"]'
            results["desc"] = WebUtil.get_web_static_info(response,desc_xpath)
            #print '景区信息：',results["desc"]
            #景区小贴士
            scenic_littletips_xpath = '//div[@class="c-wrapper no-border-top layoutfix"]'
            results["tips"] = WebUtil.get_web_static_info(response,scenic_littletips_xpath)
           # print '景区小贴士：',results["tips"]
            #开放时间
            opening_hours_xpath = '//li[@class="time"]/span[@class="j-limit"]/text()'
            opening_hours = sel.xpath(opening_hours_xpath).extract()
            if len(opening_hours) > 0 :
                for string in opening_hours:
                    results["opening_hours"] = results["opening_hours"].strip() + string.strip()
           # print "开放时间：",results["opening_hours"]
            # 好评率
            # goodCommentRate
            good_comment_rate_xpath = '//div[@class="grade"]/i/text()'
            good_comment_rate = sel.xpath(good_comment_rate_xpath).extract()
            if len(good_comment_rate) > 0:
                for string in good_comment_rate:
                    results["good_comment_rate"] = string + u"分/5分"
           # print u"好评率：", results['good_comment_rate']
            # 总评论人数
            total_comment_count_xpath = '//div[@class="grade"]/a/text()'
            total_comment_count = sel.xpath(total_comment_count_xpath).extract()
            if len(total_comment_count) > 0:
                for string in total_comment_count:
                    s1 = ""
                    for s2 in string:
                        if s2 == u"查" or s2 == u"看" or s2 == u"条" or s2 == u"评" or s2 == u"点":
                            pass
                        else:
                            s1 = s1.strip() + s2.strip()
                    results["total_comment_count"] += s1 + " "
            #print "总评论数;", results['total_comment_count']
            #经纬度
            position_xpath = "//script[6]/text()"
            position_string = sel.xpath(position_xpath).extract()
            s1 = ""
            for s2 in position_string:
                if len(s2) > 0:
                    s1 = s1.strip() + s2.strip()
            position_index_start = s1.find("position",0)
            position_index_end = position_index_start+46
            s3 = s1[position_index_start:position_index_end].strip()
            s3 = s3[10:]
            s3_list = s3.split(",")
            s3 = "".join(s3_list).strip()
            s3_list = s3.split("'")
            s3 = "".join(s3_list).strip()
            position_list = s3.split("|")
            try:
              #  float(position_list[1].strip()) #如果不能转换成数字，说明有问题，需进一步解析
                position_list_lat = position_list[1].split(" ")
                position_list[1] = position_list_lat[0].strip()
            except :
                pass
            else:
                results["longitude"] = position_list[0].strip()
                results["latitude"] = position_list[1].strip()
              #  print "经度：",results["longitude"]," 纬度：",results["latitude"]

                # 附近景区
                item["results"] = results
                surround_scenic_url = "http://piao.ctrip.com/Thingstodo-Booking-ShoppingWebSite/api/TicketDetailApi/action/GetNearbyScenicSpots?lo="+results["longitude"]+"&la="+results["latitude"].strip().encode("utf-8")
                yield  Request(surround_scenic_url,callback=self.parse_surround_scenic,dont_filter=True,meta={'item':item})
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_surround_scenic(self,response):
        try:
            item = response.meta["item"]
            results = item["results"]
            comment = json.loads(response.body)
            self.logger_data.info(json.dumps(comment))
            comment_data = comment["data"]
            try:
                if comment_data[0]["name"] != None:
                    results["surround_scenic"] = results["surround_scenic"].strip() + "\t" +comment_data[0]["name"].strip()
                    if comment_data[1]["name"] != None:
                        results["surround_scenic"] = results["surround_scenic"].strip() + "\t" + comment_data[1]["name"].strip()
                        if comment_data[2]["name"] != None:
                            results["surround_scenic"] = results["surround_scenic"].strip() + "\t" + comment_data[2]["name"].strip()
                            if comment_data[3]["name"] != None:
                                results["surround_scenic"] = results["surround_scenic"].strip() + "\t" + comment_data[3]["name"].strip()
                                if comment_data[4]["name"] != None:
                                    results["surround_scenic"] = results["surround_scenic"].strip() + "\t" + comment_data[4]["name"].strip()
            except Exception,e:
                pass
            #print "周边景区：",results["surround_scenic"]
            scenic_picture_url = "http://piao.ctrip.com/Thingstodo-Booking-ShoppingWebSite/api/TicketDetailApi/action/GetMultimedia?scenicSpotId="+results["scenic_id"][1:]
            yield Request(scenic_picture_url, callback=self.parse_scenic_picture_scenic, dont_filter=True,meta={'item': item})
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['results']['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    def parse_scenic_picture_scenic(self,response):
        try:
            item = response.meta["item"]
            results = item["results"]
            comment = json.loads(response.body)
            self.logger_data.info(json.dumps(comment))
            try:
                if comment[0] != None:
                    results["picture_url"] = results["picture_url"].strip() + "\t" + comment[0]["Ga"][0]["Url"].strip()
                    if comment[1] != None:
                        results["picture_url"] = results["picture_url"].strip() + "\t" + comment[1]["Ga"][0]["Url"].strip()
                        if comment[2] != None:
                            results["picture_url"] = results["picture_url"].strip() + "\t" + comment[2]["Ga"][0]["Url"].strip()
                            if comment[3] != None:
                                results["picture_url"] = results["picture_url"].strip() + "\t" + comment[3]["Ga"][0]["Url"].strip()
                                if comment[4] != None:
                                    results["picture_url"] = results["picture_url"].strip() + "\t" + comment[4]["Ga"][0]["Url"].strip()
                                    if comment[5] != None:
                                        results["picture_url"] = results["picture_url"].strip() + "\t" + comment[5]["Ga"][0]["Url"].strip()
                                        if comment[6] != None:
                                            results["picture_url"] = results["picture_url"].strip() + "\t" + comment[6]["Ga"][0]["Url"].strip()
                                            if comment[7] != None:
                                                results["picture_url"] = results["picture_url"].strip() + "\t" +  comment[7]["Ga"][0]["Url"].strip()
                                                if comment[8] != None:
                                                    results["picture_url"] = results["picture_url"].strip() + "\t" + comment[8]["Ga"][0]["Url"].strip()
                                                    if comment[9] != None:
                                                        results["picture_url"] = results["picture_url"].strip() + "\t" + comment[9]["Ga"][0]["Url"].strip()
                                                        if comment[10] != None:
                                                            results["picture_url"] = results["picture_url"].strip() + "\t" +  comment[10]["Ga"][0]["Url"].strip()
                                                            if comment[11] != None:
                                                                results["picture_url"] = results["picture_url"].strip() + "\t" + comment[11]["Ga"][0]["Url"].strip()
                                                                if comment[12] != None:
                                                                    results["picture_url"] = results["picture_url"].strip() + "\t" + comment[12]["Ga"][0]["Url"].strip()
                                                                    if comment[13] != None:
                                                                        results["picture_url"] = results["picture_url"].strip() + "\t" + comment[13]["Ga"][0]["Url"].strip()
                                                                        if comment[14] != None:
                                                                            results["picture_url"] = results["picture_url"].strip() + "\t" + comment[14]["Ga"][0]["Url"].strip()
                                                                            if comment[15] != None:
                                                                                results["picture_url"] = results["picture_url"].strip() + "\t" + comment[15]["Ga"][0]["Url"].strip()
            except Exception, e:
                pass
            yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['results']['scenic_id']
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