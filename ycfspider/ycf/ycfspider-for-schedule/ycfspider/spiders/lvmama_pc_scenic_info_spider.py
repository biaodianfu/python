# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from scrapy.http import Request
from scrapy.selector import Selector
from ycfspider.items import YcfspiderItem
from ycfspider.utils.webutil import WebUtil
import datetime
import time
from ycfspider.utils.redisutils import RedisUtil
import  requests,copy
import traceback,os
import json
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum
from ycfspider.tables.scenic_info_table import scenic_info_table
from scrapy.exceptions import DontCloseSpider

class LvmamaPcScenicInfoSpider(RedisSpider):
    name = "LvmamaPcScenicInfoSpider"
    allowed_domains = ["ticket.lvmama.com"]
    start_urls = []
    #redis_key = "spider:lvmama_pc_scenic_info_all"
    #设置每个最大翻页数，测试用
    dict_scenic = []
    page_num = 500

    #记录城市列表翻页数
    city_hotel_num = {}
    #pc版请求头
    cookie = 'uid=wKgKcFehRclRXwmLGmCxAg==; _lvTrack_u_ud=F47E2DBC-06F5-4906-9955-A348B8F3537A; CoreID6=93621246763214701869560&ci=90409730; chanapp_cookie=chanapp_cookie; syappfoot_cookie=syappfoot_cookie; TUANGOU_DETAIL_PRODUCTID=%5B%7B%22imageUrl%22%3A%22http%3A%2F%2Fpic.lvmama.com%2F%2Fuploads%2Fpc%2Fplace2%2F2015-06-09%2F372626c9-8f70-41b0-8765-b6b8944c23bd.jpg%22%2C%22name%22%3A%22%E7%8F%A0%E6%B5%B7%E9%95%BF%E9%9A%86%E6%B5%B7%E6%B4%8B%E7%8E%8B%E5%9B%BD%E6%B5%B7%E6%B4%8B%E7%8E%8B%E5%9B%BD%E4%B8%A4%E6%97%A5%E7%A5%A8%2B%E6%B5%B7%E5%BA%95%E5%A4%9C%E5%AE%BF%E4%B8%80%E6%99%9A-%E8%8A%82%E5%81%87%E6%97%A5%E5%8F%8C%E4%BA%BA%E5%A5%97%E7%A5%A8%22%2C%22placeId%22%3A%22http%3A%2F%2Fwww.lvmama.com%2Ftuangou%2Fdeal-v3125913%22%2C%22productsPrice%22%3A%221376%22%7D%5D; _jzqx=1.1470192440.1470800066.6.jzqsr=lvmama%2Ecom|jzqct=/.jzqsr=c%2Eduomai%2Ecom|jzqct=/track%2Ephp; oIC=067549084213058172061585; oIT=0704070507100710; feedback=196908_352_0_ZHNwNDQtOA%253D%253D_1; Hm_lvt_006c64491cb8acf2092ce0e0341797fe=1470299136,1470373637,1470817770; _gscu_1059159971=70299136cou9y729; _jzqckmp=1; cid=5740; wi=ODIxNzkyfDAwYzE3OTM2ZmMzYzQ2ZDZiZDM0; lvsessionid=254beb01-3a6e-410f-b455-9e0c9a110e30; JSESSIONID=6FD60AC11B491008F184B64901504A26; _lvTrack_u_sd=CDF3FC67-5335-458C-811B-76A75CFAB264; _lvTrack_firstVisitTime=1470910091802; Rvyz72RO3yiChuCn=RRyLY%2BNxa5qrzFG0M87mncdr0zouorKDV6L4LTRxOBjRRLztlWJGBe3vSp7TwOZpLtOIzgrxR8rcVOQixiMzaC%2FpPt2Mob4OW0Na7ieODR9eChvDPHPu1zyHhbuytsVlxMVncwA96vkrTJwNUPcput9kBLCsXNmmxmPRimx%2B%2B953U72uqBEeUC1%2Bdv5OPsJ9oK1tG8qS8THkI9OwAgDQQvbh8x5Mj0kTj5yAZg3GW05RfbzzcOpQj4dA%2FA2qmraM0n1m8fcjUkJK99Gekb2cwcTBFyqYM9pNI0vKcxyb1ebQ4txFi8hIg3Xeowy9GAxAlelXvM5wRXntGWbGA%2F7sE1f6USO7%2BYBUUYtzQLtAXOqmieIPA%2BHvJ8HlAOLY9qni2AT0CUR8QfIRjmO7hTSyFJnKNRrL6B7P4a0q1gtc0zMAV%2BmoSI9xGAEk549ecXqmQF0uxcXV2dPXfxg7E4d0Wg%3D%3D248d096749773c57b43d5c9ebbb32304f82db8b6; _jzqy=1.1470186956.1470910094.4.jzqsr=baidu|jzqct=%E9%A9%B4%E5%A6%88%E5%A6%88.jzqsr=baidu|jzqct=%E9%A9%B4%E5%A6%88%E5%A6%88; oUC=017878045060017878017878; oUT=0703071107110711; _lvTrack_preVisitTime=1470910098551; Hm_lvt_cb09ebb4692b521604e77f4bf0a61013=1470817731,1470904551,1470910094,1470910099; Hm_lpvt_cb09ebb4692b521604e77f4bf0a61013=1470910099; _pzfxuvpc=1470186956240%7C9505629621131307038%7C171%7C1470910099015%7C23%7C1892275623793851070%7C1692935053141308654; _pzfxsvpc=1692935053141308654%7C1470910093911%7C2%7Chttp%3A%2F%2Fbzclk.baidu.com%2Fadrc.php%3Ft%3D06KL00c00fZi0k60Z6-N0A0GQ0acHJkg00000aqgf1300000V1-irO.THLaEtQCs860UWdBmy-bIfK15H6vmvf3mH-bnj0srjmzPvf0IHd7rjf1nD7KPH6LwbmdfRPjwWujPjRsnYwKfYRvnjFAw6K95gTqFhdWpyfqnWT3rjb1PHn1PBusThqbpyfqnHm0uHdCIZwsrBtEILILQhkvUy7Vmi4WUvYOUAq1m1YsnHT3P16hmvdxUydW5yF9pywdQNtVmLKYQNtVXh9dmy4kIidxQgKWFMNYUNq1ULNzmvRqmh7GuZRhIgwVgvd-uA-dUHdlpZN9UM7dFMNYUNqWmydsmy-MUWdWTZf0mLFW5HndPjDk%26tpl%3Dtpl_10085_12986_1%26l%3D1044600623%26ie%3Dutf-8%26f%3D8%26tn%3Dbaidu%26wd%3D%25E9%25A9%25B4%25E5%25A6%2588%25E5%25A6%2588%26rqlang%3Dcn%26inputT%3D1420; __utma=30114658.906545313.1470904549.1470904551.1470910094.3; __utmb=30114658.2.10.1470910094; __utmc=30114658; __utmz=30114658.1470910094.3.3.utmcsr=baidu|utmccn=cpt|utmcmd=zhuanqu|utmctr=%E9%A9%B4%E5%A6%88%E5%A6%88; __xsptplus443=443.21.1470910099.1470910099.1%231%7Cbaidu%7Czhuanqu%7Ccpt%7C%25E9%25A9%25B4%25E5%25A6%2588%25E5%25A6%2588%7C%23%23l1uY9Rz58B3tURZoU6YzPj4KSphc9Buq%23; _jzqa=1.423159857345021100.1470186956.1470904549.1470910094.21; _jzqc=1; _qzja=1.884773179.1470186956341.1470904551293.1470910094002.1470910094002.1470910099143.0.0.0.45.19; _qzjb=1.1470910094002.2.0.0.0; _qzjc=1; _qzjto=3.2.0; bfd_s=30114658.24503326.1470910094046; tmc=2.30114658.28816883.1470910094049.1470910094049.1470910099214; tma=30114658.74150920.1470186956423.1470793010986.1470904549195.6; tmd=171.30114658.74150920.1470186956423.; _jzqb=1.2.10.1470910094.1; bfd_g=9de2782bcb754fd700003123000075a45791966e; cm_mmc=CPS-_-lingke-_-a-_-a; utm_source=LINKTECH; utm_medium=CPS; losc=045050; source=LINKTECH; serviceId="http://www.lvmama.com/"; ltInfo=A100222286lh_qy1nu; landingTime="2016-08-11 06:08:20"; cpsId=460d3e73843b3f6d216d66f645b1802f; orderFromChannel=LINKTECH; ip_from_place_id=229; ip_from_place_name=%E5%B9%BF%E5%B7%9E; ip_area_location=GZ; ip_location=121.33.210.162; ip_province_place_id=440000; ip_city_place_id=440100; ip_city_name=%E5%B9%BF%E5%B7%9E; abTest=B; fingerprint=678ebdecc7196b7450de5ba8e37cbe18'
    headers = {'cookie': cookie,
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate, sdch',
               'Accept-Language': 'zh-CN,zh;q=0.8',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Host': 'ticket.lvmama.com',
               'Upgrade-Insecure-Requests': 1
               }
    #整合修改
    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:lvmama_pc_scenic_info_all'
    else:
        redis_key = 'spider:lvmama_pc_scenic_info_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(LvmamaPcScenicInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)
    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider


    # lvmama_pc_scenic_info_logger = Logger(settings.get('LOG_PATH') + '/' + name + '/original')
    # lvmama_pc_scenic_info_error_logger = Logger(settings.get('LOG_PATH') + '/' + name + 'error/original')

    def next_request(self):
        s = requests.session()
        s.keep_alive = False
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            url = 'http://ticket.lvmama.com/scenic-'+str(item["scenic_id"])
            return Request(url,meta={'item':item},headers=self.headers,dont_filter=True)

    def parse(self, response):
        try:
            sel = Selector(response)
            item = YcfspiderItem()
            body = response.body
            self.logger_data.info(body)
            data = response.meta['item']
            item["kafka_url"] = settings.get("KAFKA_ADDRESS") + settings.get("KAFKA_SCENIC_INFO_RESOURSE_PATH")
            # results = {"id":"","scenic_id":data["scenic_id"],"scenic_name":data["scenic_name"],"scenic_used_name":"","platform_id":1,"channel_id":7,"scenic_belongs":0,"group_belongs":"","country_code":"CN","country_name":"china","province_code":"","province_name":"","city_code":"","city_name_cn":data["city_name"],"city_name_gb":"","district_code":"","district_name":"","short_address":"",
            #            "address":data["address"],"postal_code":"","position_type":u"经纬度","longitude":"","latitude":"","telephone":"","email":"","fax":"","url":"","picture_url":"","picture_list_url":"","desc":"","scenic_ad":"","scenic_notice":"","type":"","level":"","score":"","currency":"","ticket_price":"","min_price":"","tips":"","comment_desc":"",
            #            "total_comment_count":"","good_comment_rate":"","favourite_count":"","praise_count":"","share_count":"","opening_hours":"","policy":"","is_credit_card_supportted":"","facility":"","scenic_service":"","has_parking_lot":"","has_wifi":"","has_atm":"","has_restaurant":"","has_gym":"","shared_url":"","active_facility":"",
            #            "surround_airport":"","surround_metro":"","surround_railway_station":"","surround_bus_station":"","surround_rent_car":"","surround_scenic":"","surround_hotel":"","surround_shopping":"","surround_food":"","surround_entertainment":"","status":"","other_welfare":"","remark":"","keywords":"","reserved_col1":"","reserved_col2":"","reserved_col3":"","reserved_col4":"",
            #            "reserved_col5":"","crawl_version":"","crawl_time":time.strftime("%Y-%m-%d %H:%M:%S")}
            results = copy.deepcopy(scenic_info_table)
            results['scenic_id'] = data["scenic_id"]
            results['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            results['platform_id'] = 1
            results['channel_id'] = 7
            scenic_name_xpath = "//div[@class='titbox']/h1[@class='tit']/text()"
            scenic_name = sel.xpath(scenic_name_xpath).extract()
            if scenic_name:
                results["scenic_name"] = scenic_name[0]
                # print "景区名称：",results["scenic_name"]
            address_xpath = "//dl[@class='dl-hor']/dd/p/text()"
            address = sel.xpath(address_xpath).extract()
            if address:
                results["address"] = address[0]
                # print "景区地址：",results["address"]
            city_name_cn_xpath = "//span[@class='crumbs-list']/a[@class='crumbs-down-a']/text()"
            city_name_cn = sel.xpath(city_name_cn_xpath).extract()
            if city_name_cn:
                results["city_name_cn"] = city_name_cn[1][:-4]
                # print "城市中文名称：",results["city_name_cn"]
            #交通工具
            surround_traffic_nav_xpath = '//div[@class="nchTrafficNav"]/ul/li/b/text()'
            surround_traffic_list_xpath = '//div[@class="nchTrafficProgra"]'
            surround_traffic_nav = sel.xpath(surround_traffic_nav_xpath).extract()
            surround_traffic_list = sel.xpath(surround_traffic_list_xpath).extract()
            if len(surround_traffic_nav) > 0:
                for i in range(0,len(surround_traffic_nav)):
                    surround_traffic_list_c_xpath = surround_traffic_list_xpath + '['+str(i+1)+']/p/text()'
                    surround_traffic_list_c = sel.xpath(surround_traffic_list_c_xpath).extract()
                    if surround_traffic_nav[i] == u"轨道交通":
                        #results['surround_metro'] = results['surround_metro']+surround_traffic_list_c[i]
                        for string in surround_traffic_list_c:
                            results['surround_metro'] = results['surround_metro'] + string
                    if surround_traffic_nav[i] == u"公交车" or surround_traffic_nav[i] == u'交通速览':
                        for string in surround_traffic_list_c:
                            results['surround_bus_station'] = results['surround_bus_station'] + string
                    if surround_traffic_nav[i] == u"出租车" or surround_traffic_nav[i] == u'交通信息':
                        for string in surround_traffic_list_c:
                            results['surround_bus_station'] = results['surround_bus_station'] + string
                    if surround_traffic_nav[i] == u"免费巴士" or surround_traffic_nav[i] == u'旅游大巴' or surround_traffic_nav[i] == u'旅游专车':
                        for string in surround_traffic_list_c:
                            results['surround_bus_station'] = results['surround_bus_station'] + string
                    if surround_traffic_nav[i] == u"租车" or surround_traffic_nav[i] == u"汽车" or surround_traffic_nav[i] == u'自驾车':
                        for string in surround_traffic_list_c:
                            results['surround_bus_station'] = results['surround_bus_station'] + string
            #print "汽车：",results['surround_bus_station']
            #print "轨道交通;",results['surround_metro']

            #所属省名
            province_name_xpath = '//span[@class="crumbs-list"]/a[@class="crumbs-down-a"]/text()'
            province_name_x = sel.xpath(province_name_xpath).extract()
            if len(province_name_x) > 0 :
                province_name_list = province_name_x.pop(0)
                province_name = province_name_list[0:3]
                if province_name == u"黑龙江" or province_name == u"内蒙古":
                    pass
                else:
                    province_name = province_name[0:2]
                results['province_name'] = province_name
                #print u"省份:",results['province_name']
            #景区URl
            results["url"] = response.url
           # print u"景区url:",results["url"]
            #景区图片url
            picture_url_list_xpath = '//ul[@class="pic_mod_ul"]/li'
            picture_url_list = sel.xpath(picture_url_list_xpath).extract()
            for i in range(len(picture_url_list)):
                picture_url_c_xpath = picture_url_list_xpath + '['+str(i+1)+']/img/@src'
                picture_url_c = sel.xpath(picture_url_c_xpath).extract()
                results["picture_url"] = results["picture_url"] + picture_url_c[0]+" "
           # print  u"景区图片URL:",results["picture_url"]
            #景区描述
            # desc_xpath = '//div[@class="dcontent"]/p'
            # desc = sel.xpath(desc_xpath).extract()
            # for i in range(len(desc)):
            #     desc_c_xpath = desc_xpath + '['+str(i+1)+']/text()'
            #     desc_c = sel.xpath(desc_c_xpath).extract()
            #     for string in desc_c:
            #         results["desc"] = results["desc"] + "\n" +string
            desc_xpath = '//div[@class="dcontent"]'
            results['desc'] = WebUtil.get_web_static_info(response,desc_xpath)
            #print u"景区描述：",results['desc']
            #小贴士
            # scenic_littletips_xpath = '//div[@class="darea noline"]/p'
            # scenic_littletips = sel.xpath(scenic_littletips_xpath).extract()
            # for i in range(len(scenic_littletips)):
            #     scenic_littletips_c_xpath = scenic_littletips_xpath + '[' + str(i + 1) + ']/text()'
            #     scenic_littletips_c = sel.xpath(scenic_littletips_c_xpath).extract()
            #     for string in scenic_littletips_c:
            #          results["tips"] = results["tips"] + string
            scenic_littletips_xpath = '//div[@class="darea noline"]'
            results['tips'] = WebUtil.get_web_static_info(response,scenic_littletips_xpath)
           # print u"小贴士：",results['tips']
            #营业时间
            # opening_hours_xpath = '//dl[@class="dl-hor index3"]/dd[@class="xlesstime"]/p/text()'
            # opening_hours = sel.xpath(opening_hours_xpath).extract()
            # if len(opening_hours_xpath) > 0:
            #     for string in opening_hours:
            #         results["opening_hours"] = string
            opening_hours_xpath = '//dl[@class="dl-hor index3"]/dd[@class="xlesstime"]/p'
            results["opening_hours"] = WebUtil.get_web_static_info(response,opening_hours_xpath)
            #print u"营业时间：",results["opening_hours"]
            #景区等级
            # level_xpath = '//div[@class="titbox"]/span[@class="mp_star"]/i/text()'
            # level = sel.xpath(level_xpath).extract()
            # if len(level) > 0:
            #     for string in level:
            #         results["level"] = string + u'级景区'
            level_xpath = '//div[@class="titbox"]/span[@class="mp_star"]'
            results["level"] = WebUtil.get_web_static_info(response, level_xpath)
            #print u"景区等级：",results["level"]

            #好评率
            #goodCommentRate
            good_comment_rate_xpath = '//span/i[@data-mark="dynamicNum"]/text()'
            good_comment_rate = sel.xpath(good_comment_rate_xpath).extract()
            if len(good_comment_rate) > 0:
                for string in good_comment_rate:
                    results["good_comment_rate"] = string + "%"
            #print u"好评率：",results['good_comment_rate']
            #总评论人数
            total_comment_count_xpath = '//*/a[@href="#yhdp"]/text()'
            total_comment_count = sel.xpath(total_comment_count_xpath).extract()
            if len(total_comment_count) > 0:
                for string in total_comment_count:
                    s1 = ""
                    for s2 in string:
                        if s2 == u"查" or s2 == u"看" or s2 == u"人" or s2 == u"评" or s2 == u"点":
                            pass
                        else:
                            s1 = s1.strip() + s2.strip()
                    results["total_comment_count"] += s1
            ##print "总评论数;",results['total_comment_count']

            #评论 暂未实现
            # comment_desc_xpath = '//*[@class="quote"]/ul[@class="pj_list"]/li'
            # comment_desc = sel.xpath(comment_desc_xpath).extract()
            # print comment_desc
            # for i in range(len(comment_desc)):
            #     comment_desc_c_xpath = comment_desc_xpath + '['+str(i+1)+']/@title'
            #     comment_desc_c = sel.xpath(comment_desc_c_xpath).extract()
            #     if len(comment_desc_c) > 0:
            #         for string in comment_desc_c:
            #             results['comment_desc'] = results['comment_desc'] + "\n" + string
            # print "评论：",results['comment_desc']

            #附近景区
            surround_scenic_xpath = '//div[@class="guess-main"]/ul[@id="guess_list_ticket"]/li'
            surround_scenic = sel.xpath(surround_scenic_xpath).extract()
            if len(surround_scenic) > 0:
                for i in range(len(surround_scenic)):
                    surround_scenic_c_path = surround_scenic_xpath + '['+str(i+1)+']/h5/a/text()'
                    surround_scenic_c = sel.xpath(surround_scenic_c_path).extract()
                    for string in  surround_scenic_c:
                        results["surround_scenic"] = results["surround_scenic"]  +string +"\t"
            #print "附近景区;",results["surround_scenic"]

            #附近酒店
            surround_hotel_xpath = '//div[@class="guess-main"]/ul[@id="hotel_list_ticket"]/li'
            surround_hotel = sel.xpath(surround_hotel_xpath).extract()
            if len(surround_hotel) > 0:
                for i in range(len(surround_hotel)):
                    surround_hotel_c_path = surround_hotel_xpath + '[' + str(i + 1) + ']/h5/a/text()'
                    surround_hotel_c = sel.xpath(surround_hotel_c_path).extract()
                    for string in surround_hotel_c:
                        results["surround_hotel"] = results["surround_hotel"] + string + "\t"
            #print "附近酒店;", results["surround_hotel"]

            item["results"] = results
            yield  item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.LVMAMA
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta['item']['scenic_id']
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











