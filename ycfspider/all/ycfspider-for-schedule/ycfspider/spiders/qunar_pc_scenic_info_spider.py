#coding=utf-8
import sys

from scrapy.exceptions import DontCloseSpider

reload(sys)
sys.setdefaultencoding("utf-8")
from scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from ycfspider.items import YcfspiderItem
import re,os
import json,traceback
import copy
import datetime
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.tables.scenic_info_table import scenic_info_table
from ycfspider.utils.enum import ChannelEnum,ErrorTypeEnum,PlatformEnum,CrawlTypeEnum

inter_start = '<!--网络设施:star-->'
inter_end = '<!--网络设施:end-->'
hotel_start = '<!--酒店设施:star-->'
hotel_end = ' <!--酒店设施:end-->'
service_start = '<!--酒店服务:star-->'
service_end = '<!--酒店服务:end-->'
stop_start = '<!--停车场 :star-->'
stop_end = '<!--停车场:end-->'
room_start = '<!--房间设施:star-->'
room_end = '<!--房间设施:end-->'

class QunarPcScenicInfoSpider(RedisSpider):
    name = 'QunarPcScenicInfoSpider'
    # 减慢爬取速度 为 2s
    allowed_domains = ["touch.qunar.com", "hotel.qunar.com"]
    start_urls = []
    hotelids = []
    # 设置每个最大翻页数，测试用
    dict_hotel = {}
    page_num = 500
    # 记录城市列表翻页数
    city_hotel_num = {}
    #pipLine = TutorialPipeline()
    # pc版请求头
    cookie = 'QN1=eIQiQleXUScORgs1ErNtAg==; QN269=55C125A052F711E69E4E6C3BE5A8881C; pgv_pvi=1293347840; QN99=312; QN48=tc_39e92ba09a2996f2_1562fe2cab8_ce4c; QN73=2493-2494; __utma=183398822.417211823.1469534507.1469763366.1469779631.7; __utmz=183398822.1469779631.7.7.utmcsr=hotel.qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/city/suzhou_jiangsu/dt-10664; csrfToken=G1TpIdkE3vrAmoD23N8JxgpTnPkHs1ZV; QunarGlobal=10.86.213.138_435e4276_15649d12280_1639|1470119069499; QN163=0; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3=1470119070; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1470119070; PHPSESSID=244iqgakdpq2gq13gd34nc5en2; QN268=|1470119070426_0aa07af9e443058a; QN71="MTIxLjMzLjIxMC4xNjI65bm/5LicOjE="; QN57=14701190746640.009358836753765587; _i=RBTKAD-rKGEx-oLTsUaIwTcNODmx; _vi=j6kcMnxJEiBbthvtQTPRCHDoKcZQLVefd8nnMQ_nuOFsJ3VylRvAfh_hwj1vlguHzZMbfyA13ncVDuGR5fhMGkBX3IsOUffdb-edDPGlgUcD-ONKYdg82C9HXF9z7mxvjYfSs2r9UAlOpSB8xh8i_I9Rq1nGcZ1ddh9R1eLPMgcs; QN58=1470119074663%7C1470120266930%7C7; JSESSIONID=59307D4D7775E5675D1C7003D4ECCA99'
    headers = {'cookie': cookie,
               'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Accept-Encoding':'gzip, deflate, sdch',
               'Accept-Language':'zh-CN,zh;q=0.8',
                'Cache-Control':'max-age=0',
                'Connection':'keep-alive',
                'Host':'piao.qunar.com',
                'Upgrade-Insecure-Requests':1
               }

    filename = settings.get('LOG_PATH') + '/' + name
    if not os.path.exists(filename):
        os.makedirs(filename)
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:qunar_pc_scenic_info_all'
    else:
        redis_key = 'spider:qunar_pc_scenic_info_ycf'

    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')

    def __init__(self, *args, **kwargs):
        super(QunarPcScenicInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)


    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            url = 'http://piao.qunar.com/ticket/detail_'+str(item["scenic_id"])+'.html'
            return  Request(url,meta={'item':item},headers=self.headers,dont_filter=True)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    # 解析酒店详情页信息
    def parse(self, response):
        try:
            self.logger_data.info(response.body)
            sel = Selector(response)
            data = response.meta["item"]
            results = copy.deepcopy(scenic_info_table)
            results['scenic_id'] = data["scenic_id"]
            results['crawl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            scenic_name_xpath = '//*[@class="mp-description-name"]/@title'
            scenic_name = sel.xpath(scenic_name_xpath).extract()
            if scenic_name:
                results['scenic_name'] = scenic_name[0]
            city_name_xpath = '//*[@name="location"]/@content'
            city_name = sel.xpath(city_name_xpath).extract()
            if city_name:
                if re.findall(u"city=[\u4e00-\u9fa5]+",city_name[0]):
                    results['city_name_cn'] = re.findall(u"city=[\u4e00-\u9fa5]+",city_name[0])[0].replace('city=','')
                    # print  results['city_name_cn']
            scenic_address_xpath = '//*[@class="mp-description-address"]/@title'
            scenic_address = sel.xpath(scenic_address_xpath).extract()
            if scenic_address:
                results['address'] = scenic_address[0]
                # print  results['address']
            #results['ticket_price'] = data["qunar_price"]
            results['currency'] = 1
            results['platform_id'] = 1
            results['channel_id'] = 2
            results['scenic_belongs'] = 0
            surround_bus_station_xpath = '//*[@class="mp-traffic-transfer"]/div[@class="mp-transfer-desc"]/p'
            surround_bus_station = sel.xpath(surround_bus_station_xpath).extract()
            if surround_bus_station:
                for i in range(0,len(surround_bus_station)):
                    surround_bus_station_p_xpath = surround_bus_station_xpath + '['+str(i+1)+']/text()'
                    surround_bus_station_p = sel.xpath(surround_bus_station_p_xpath).extract()
                    if surround_bus_station_p:
                        results["surround_bus_station"] = results["surround_bus_station"] + surround_bus_station_p[0]+'##'
            province_name_xpath = '//*[@name="location"]/@content'
            province_name = sel.xpath(province_name_xpath).extract()
            if len(province_name)>0:
                province_name = province_name[0]
                province_name_list = province_name.split(';')
                if len(province_name_list)>0:
                    pattern = re.compile(u"[\u4e00-\u9fa5]+")
                    province_name_list_0 = pattern.findall(province_name_list[0])
                    if len(province_name_list_0)>0:
                        results['province_name'] = province_name_list_0[0]
                    # lanandlng =  re.findall(r'[0-9]+.[0-9]+',province_name_list[2])
                    # if len(lanandlng) == 2:
                    #     results['longitude'] = lanandlng[0]
                    #     results['latitude'] = lanandlng[1]
            lng_lat_xpath =  '//*[@id="baidu-map-point"]/@value'
            lng_lat = sel.xpath(lng_lat_xpath).extract()
            if lng_lat:
                lanandlng =  re.findall(r'[0-9]+.[0-9]+',lng_lat[0])
                if len(lanandlng) == 2:
                    results['longitude'] = lanandlng[0]
                    results['latitude'] = lanandlng[1]
            results["url"] = response.url
            picture_url_list_xpath = '//*[@id="mp-slider-content"]/div'
            picture_url_list = sel.xpath(picture_url_list_xpath).extract()
            picture_list = []
            if len(picture_url_list)>0:
                for i in range(len(picture_url_list)):
                    picture_xpath = picture_url_list_xpath +'['+ str(i+1)+']/img/@src'
                    picture = sel.xpath(picture_xpath).extract()
                    if len(picture)>0:
                        if i == 0:
                            results["picture_url"] = picture[0]
                            picture_list.append(picture[0])
                        else:
                            picture_list.append(picture[0])
            results["picture_list_url"] = picture_list
            desc_xpath = '//*[@class="mp-charact-intro"]/div[1]/p'
            desc = sel.xpath(desc_xpath).extract()
            p_content = ''
            if len(desc)>0:
                for i in range(len(desc)):
                    p_xpath = desc_xpath+'['+str(i+1)+']/text()'
                    p = sel.xpath(p_xpath).extract()
                    if p:
                        p_content = p_content + p[0]+'##'
            results["desc"] = p_content
            scenic_ad_xpath = '//*[@class="mp-description-onesentence"]/text()'
            scenic_ad = sel.xpath(scenic_ad_xpath).extract()
            if scenic_ad:
                results["scenic_ad"]=scenic_ad[0]

            scenic_littletips_xpath = '//*[@id="mp-charact"]/div[@class="mp-charact-littletips"]'
            # if 'mp-charact-littletips' in response.body:
            scenic_littletips = sel.xpath(scenic_littletips_xpath).extract()
            # else:
            #     scenic_littletips=[]
            if len(scenic_littletips)>0:
                for i in range(len(scenic_littletips)):
                     scenic_notice_title_xpath = scenic_littletips_xpath+'['+str(i+1)+']/div[1]/h2[@class="mp-littletips-title pngfix"]/text()'
                     scenic_notice_title = sel.xpath(scenic_notice_title_xpath).extract()
                     if '入园公告' in scenic_notice_title:
                         scenic_notice_1 = ''
                         scenic_notice_xpath = scenic_littletips_xpath+'['+str(i+1)+']/div[1]/div[@class="mp-littletips-item"]'
                         scenic_notice = sel.xpath(scenic_notice_xpath).extract()
                         if scenic_notice:
                             for j in range(len(scenic_notice)):
                                 scenic_notice_p = ''
                                 scenic_notice_content_xpath = scenic_notice_xpath +'['+ str(j+1)+']/div[@class="mp-littletips-desc"]/p'
                                 scenic_notice_content = sel.xpath(scenic_notice_content_xpath).extract()
                                 if scenic_notice_content:
                                     for k in range(len(scenic_notice_content)):
                                         scenic_notice_content_p_xpath = scenic_notice_content_xpath+'['+str(k+1)+']/text()'
                                         scenic_notice_content_p = sel.xpath(scenic_notice_content_p_xpath).extract()
                                         if scenic_notice_content_p:
                                             scenic_notice_p = scenic_notice_p + scenic_notice_content_p[0] + ';'
                                 scenic_notice_1 = scenic_notice_1 + scenic_notice_p + '##'
                         results["scenic_notice"] = scenic_notice_1
                     if '小贴士' in scenic_notice_title:
                         scenic_notice_2 = ''
                         scenic_notice_xpath = scenic_littletips_xpath+'['+str(i+1)+']/div[1]/div[@class="mp-littletips-item"]'
                         scenic_notice = sel.xpath(scenic_notice_xpath).extract()
                         if scenic_notice:
                             for j in range(len(scenic_notice)):
                                 scenic_notice_p = ''
                                 scenic_notice_content_xpath = scenic_notice_xpath +'['+ str(j+1)+']/div[@class="mp-littletips-desc"]/p'
                                 scenic_notice_content = sel.xpath(scenic_notice_content_xpath).extract()
                                 if scenic_notice_content:
                                     for k in range(len(scenic_notice_content)):
                                         scenic_notice_content_p_xpath = scenic_notice_content_xpath+'['+str(k+1)+']/text()'
                                         scenic_notice_content_p = sel.xpath(scenic_notice_content_p_xpath).extract()
                                         if scenic_notice_content_p:
                                             scenic_notice_p = scenic_notice_p + scenic_notice_content_p[0] + ';'
                                 scenic_notice_2 = scenic_notice_2 + scenic_notice_p + '##'
                         results["tips"] = scenic_notice_2
            opening_hours_xpath_1 = '//*[@class="mp-charact-time"]/div[@class="mp-charact-content"]/div[@class="mp-charact-desc"]/p/text()'
            opening_hours_xpath_2 = '//*[@class="mp-charact-time"]/div[@class="mp-charact-content"]/div[@class="mp-charact-desc"]/p/span'
            opening_hours_1 = sel.xpath(opening_hours_xpath_1).extract()
            opening_hours_2 = sel.xpath(opening_hours_xpath_2).extract()
            opening_hours_content = ''
            if opening_hours_1:
                opening_hours_content = opening_hours_content + opening_hours_1[0] + ';'
            if opening_hours_2:
                for i in range(len(opening_hours_2)):
                    opening_hours_p_xpath_2 = opening_hours_xpath_2+'['+str(i+1)+']/text()'
                    opening_hours_p = sel.xpath(opening_hours_p_xpath_2).extract()
                    if opening_hours_p:
                        opening_hours_content = opening_hours_content + opening_hours_p[0] + ';'
            results["opening_hours"] = opening_hours_content

            level_xpath = '//*[@class="mp-description-level"]/text()'
            level = sel.xpath(level_xpath).extract()
            if level:
                results["level"] = level[0]
            #解析评论分数ID，获取data-sightId,组装url，获得相应链接，然后根据链接中获得的链接，再次请求，然后解析即可得到
            data_sightId_xpath = '//*[@id="mp-tickets"]/@data-sightid'
            data_sightId = sel.xpath(data_sightId_xpath).extract()
            if data_sightId:
                data_sight_id = data_sightId[0]
                #url = 'http://travel.qunar.com/place/api/menpiao/poiInfo?callback=jQuery17208603465606237578_1470193520701&poi='+str(data_sight_id)
                url = 'http://travel.qunar.com/place/api/menpiao/poiInfo?poi='+str(data_sight_id)
                yield Request(url,callback=self.parse_comment,dont_filter=True,meta={'results':results,'data_sight_id':data_sight_id})
            #yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["item"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_comment(self,response):
        try:
            results = response.meta["results"]
            comment = json.loads(response.body)
            self.logger_data.info(json.dumps(comment))
            comment_data = comment["data"]
            if 'commentCount' in comment_data.keys():
                 results["total_comment_count"]  = comment_data["commentCount"]
            if 'commentsUrl' in comment_data.keys():
                comments_url = comment["data"]["commentsUrl"]
                yield Request(comments_url,dont_filter=True,callback=self.parse_score,meta={'results':results,'data_sight_id':response.meta['data_sight_id']})
            else :
                url = 'http://tagnt.qunar.com/partner/detail/recommend.action?callback=&cityName='+ results["city_name_cn"] + '&baiduLongitude='+ results['longitude'] + '&baiduLatitude='+  results['latitude'] + '&pageSize=4&in_track=menpiao_ads_home'
                yield Request(url,callback=self.parse_sur_hotel,dont_filter=True,meta={'results':results,'data_sight_id':response.meta['data_sight_id']})
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_score(self,response):
        try:
            sel = Selector(response)
            self.logger_data.info(response.body)
            results = response.meta["results"]
            score_xpath = '//*[@class="score"]/text()'
            score = sel.xpath(score_xpath).extract()
            if score:
                results["score"] = score[0]
            good_comment_rate = '//*[@class="scorebox clrfix"]/span[@class="total_star"]/span[1]/@style'
            good_comment = sel.xpath(good_comment_rate).extract()
            if good_comment:
                if re.findall(r'[0-9]+.',good_comment[0]):
                    results["good_comment_rate"] = re.findall(r'[0-9]+.',good_comment[0])[0]
            url = 'http://tagnt.qunar.com/partner/detail/recommend.action?callback=&cityName='+ results["city_name_cn"] + '&baiduLongitude='+ results['longitude'] + '&baiduLatitude='+  results['latitude'] + '&pageSize=4&in_track=menpiao_ads_home'
            yield Request(url,callback=self.parse_sur_hotel,dont_filter=True,meta={'results':results,'data_sight_id':response.meta['data_sight_id']})
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_sur_hotel(self,response):
        try:
            results = response.meta["results"]
            body = response.body
            self.logger_data.info(body)
            comment = json.loads(response.body[1:-1])
            if 'hotels' in comment.keys():
                hotel_list = comment["hotels"]
                sur_hotel = ''
                if hotel_list:
                    for hotel in hotel_list:
                        if 'hotelTitle' in hotel.keys():
                            sur_hotel =sur_hotel + hotel["hotelTitle"] + ';'
                        if 'distance' in hotel.keys():
                            sur_hotel = sur_hotel + '距离'+  str(hotel["distance"]) + 'm;'
                        if 'price' in hotel.keys():
                             sur_hotel = sur_hotel + '价格'+  str(hotel["price"]) + '元起;'
                        sur_hotel = sur_hotel + '##'
                    results["surround_hotel"] = sur_hotel
            url = 'http://search.piao.qunar.com/recommend/detail/recommendSight.jsonp?callback=&id=' + str(response.meta['data_sight_id'])
            yield Request(url,callback=self.parse_sur_scenic,dont_filter=True,meta={'results':results})
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_sur_scenic(self,response):
        try:
            item = YcfspiderItem()
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_SCENIC_INFO_RESOURSE_PATH')
            results = response.meta["results"]
            body  = response.body
            self.logger_data.info(body)
            comment = json.loads(response.body[1:-2])
            if 'data' in comment.keys():
                if  'arroundSights' in comment["data"].keys():
                     scenic_list = comment["data"]["arroundSights"]
            sur_scenic = ''
            for scenic in scenic_list:
                if 'name' in scenic.keys():
                    sur_scenic = sur_scenic + scenic['name'] + ';'
                if 'qunarPrice' in  scenic.keys():
                    sur_scenic = sur_scenic +'qunar价格'+ str(scenic['qunarPrice']) + '元起;'
                if 'marketPrice' in scenic.keys():
                    sur_scenic = sur_scenic +'市场价格'+ str(scenic['qunarPrice']) + '元起;'
                sur_scenic = sur_scenic + '##'
            results["surround_scenic"] = sur_scenic
            item['results'] = results
            #c=121,city_id
            #url ='http://api.map.baidu.com/?qt=s&c=121&wd=火车站&rn=10&ie=utf-8&oue=1&fromproduct=jsapi&res=api&callback=BMap._rd._cbk51207&ak=D0ba2606b334fb2565df2646e9f8a479'
            #url='http://api.map.baidu.com/?qt=s&c=121&wd=飞机场&rn=10&ie=utf-8&oue=1&fromproduct=jsapi&res=api&callback=BMap._rd._cbk63959&ak=D0ba2606b334fb2565df2646e9f8a479'
            #yield Request(url,callback=self.parse_sur_traffic,dont_filter=True,meta={'results':results})
           # print item
            yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.SCENICINFO
            error_log_dic['id'] = response.meta["results"]['scenic_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))
    #
    # def close(spider, reason):
    #         spiderStateRecord.flag_remove(spider.name)
    #         closed = getattr(spider, 'closed', None)
    #         if callable(closed):
    #             return closed(reason)
