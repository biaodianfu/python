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


class QunarPcHotelInfoSpider(RedisSpider):
    name = 'QunarPcHotelInfoSpider'
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
               'Accept - Encoding': 'gzip, deflate, sdch'
               }
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:qunar_pc_hotel_info_all'
    else:
        redis_key = 'spider:qunar_pc_hotel_info_ycf'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        super(QunarPcHotelInfoSpider, self).__init__(*args, **kwargs)
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
                    if self.redis_key == 'spider:qunar_pc_hotel_info_all':
                        return Request(item['url'], meta={'cityName': item["city"], 'id': item["id"]},
                                       headers=self.headers, dont_filter=True)
                    if self.redis_key == 'spider:qunar_pc_hotel_info_ycf':
                        l = item["hotel_id"].split('_')
                        if len(l) == 2:
                            url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0], l[1])
                            cityName = l[0]
                            id = l[1]
                        elif len(l) == 3:
                            url = 'http://hotel.qunar.com/city/%s/dt-%s' % (l[0] + "_" + l[1], l[2])
                            cityName = l[0] + "_" + l[1]
                            id = l[2]
                        return Request(url, meta={'cityName': cityName, 'id': id}, headers=self.headers,
                                       dont_filter=True)
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

    # 解析酒店详情页信息
    def parse(self, response):
        try:
            sel = Selector(response)
            item = YcfspiderItem()
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_INFO_RESOURSE_PATH')
            results = {"id": "", "desc": "", "retail_price": "", "hotel_id": response.meta['id'], "hotel_name": "",
                       "hotel_used_name": "", "address": "", "short_address": "", "channel_id": "2",
                       "hotel_belongs": '0', "country_code": "CN", "country_name": 'china', "province_code": "",
                       "province_name": "", "city_code": "", "city_name_cn": "",
                       "city_name_gb": response.meta['cityName'],
                       "district_code": "", "district_name": "", "postal_code": "", "business": "",
                       "position_type": u'经纬度', "longitude": "", "latitude": "", "telephone": "",
                       "email": "", "fax": "", "url": "", "picture_url": "", "picture_list_url": "", "comment_desc": "",
                       "brand": "", "type": "", "level": "", "score": "", "total_comment_count": "",
                       "good_comment_rate": "", "favourite_count": "", "praise_count": "", "share_count": "",
                       "opening_time": "", "decorate_time": "", "storeys": "", "rooms": "",
                       "policy": "", "check_in_time": "", "check_out_time": "", "is_pet_allowed": "",
                       "is_credit_card_supportted": "", "facility": "", "hotel_service": "", "has_parking_lot": "",
                       "has_wifi": "", "has_atm": "", "has_restaurant": "", "has_gym": "", "shared_url": "",
                       "active_facility": "", "status": "", "hotel_tag": "", "area_tag": "", "remark": "",
                       "keywords": "", "crawl_version": "", "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                       "surround_airport": "", "surround_metro": "", "surround_bus_station": "", "surround_food": "",
                       "surround_entertainment": "",
                       "surround_rent_car": "", "surround_scenic": "", "surround_shopping": "", "surround_hotel": "",
                       "platform_id": "1", "reserved_col1": "", "reserved_col2": "", "reserved_col3": "",
                       "reserved_col4": "",
                       "reserved_col5": "",  "good_comment_count": "", "general_comment_count": "", "bad_comment_count": "",
                       "avg_position_score":"","avg_facility_score":"","avg_service_score":"","avg_health_score":"",
                        "cost_effective_score":"","avg_comfort_score":"","avg_food_score":"","avg_safety_score":"","avg_scale_score":"",
                       "guest_impression":""}


            logstr = {"id": results["hotel_id"], "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      "data": response.body}
            self.logger_data.info(logstr)
            picture = sel.xpath('//*[@id="mainImg"]/a/img/@src').extract()
            picture_list_url = []
            js_picture = sel.xpath(
                '//*[@class="img js-picsRotate"]')  # //*[@id="js_headerImage_fine"]/ul[1]/li[1]/a/div
            if picture:
                results["picture_url"] = picture[0]
                picture_list_url.append(results["picture_url"])
            elif js_picture:
                if len(js_picture) >= 1:
                    for i in range(len(js_picture)):
                        xpathstr = '//*[@id="js_headerImage_fine"]/ul[1]/li[' + str(i + 1) + ']/a/div/img/@scr]'
                        results["picture_url"] = sel.xpath(xpathstr).extract()[0]
                        picture_list_url.append(results["picture_url"])
            # //*[@id="smallImages"]/li[1]
            picture_url_li = sel.xpath('//*[@id="smallImages"]/li')
            if picture_url_li:
                for i in range(len(picture_url_li)):
                    xpathli = '//*[@id="smallImages"]/li[' + str(i + 1) + ']/a'
                    if len(sel.xpath(xpathli)) > 0:  # /img/@src'
                        picture_list_url.append(sel.xpath(xpathli + '/img/@src').extract()[0])
            picture_js_small = sel.xpath('//*[@id="js_headerImage_small"]/ul/li')
            if picture_js_small:
                for i in range(len(picture_js_small)):
                    xpathjs = '//*[@id="js_headerImage_small"]/ul/li[' + str(i + 1) + ']/a/div/img/@src'
                    picture_list_url.append(sel.xpath(xpathjs).extract())
            results["picture_list_url"] = picture_list_url
            busi = sel.xpath('//*[@class="adress"]/span[1]/cite').extract()
            length = len(busi)
            if length > 0:
                businessstr = ''
                for cite in range(length):
                    xpathurl = '//*[@class="adress"]/span[1]/cite[' + str(cite + 1) + ']/a/del/text()'
                    if sel.xpath(xpathurl).extract():
                        businessstr = businessstr + sel.xpath(xpathurl).extract()[0] + '##'
                results['business'] = businessstr

            re_hotelname = 'hotelName="([^\"]*)";'
            pattern_hotel = re.compile(re_hotelname, re.S)
            match = re.search(pattern_hotel, response.body)

            if match:
                name = match.group(1)
                results['hotel_name'] = name
                # now = time.strftime("%Y-%m-%d %H:%M:%S")
                # unixnow = util.datetime_timestamp(now)
                # path = self.settings["FILEPATH"]
                # textstr =path+"qunar_data/"+response.meta['id'] + '_'+item['name']+'_'+str(unixnow)+'.txt'
                #  print '--------file----------'
                # print textstr
                # fx = open(textstr,'a')
                # fx.write(response.body)
                # fx.close()
                # re_city = 'cityName=\'([^\"]*)\';'
                # pattern_city = re.compile(re_city, re.S)
                # m = re.search(pattern_city, response.body)
                #  if m:
                #     city = m.group(1)
                # item['city'] = city
                #     item["city_name_cn"]=city
            city_name_cn_xpath = '//*[@id="js_breadcrumbs_city"]/text()'
            city_name_cn = sel.xpath(city_name_cn_xpath).extract()
            if city_name_cn:
                results["city_name_cn"] = city_name_cn[0].replace(u"酒店", "")
            # print item["city_name_cn"]
            address = sel.xpath('//*[@id="detail_pageHeader"]/p/span[1]/@title').extract()
            results['address'] = ''.join(address)
            level = sel.xpath('//*[@id="detail_pageHeader"]/h2/em/text()').extract()
            # item['level'] = ''.join(level)
            results["type"] = ''.join(level)
            telephoneandfax = sel.xpath('//*[@id="descContent"]/div/dl[1]/dd/cite')  # [1]/text()').extract()
            if len(telephoneandfax) > 0:
                if len(telephoneandfax) == 1:
                    telephone = sel.xpath('//*[@id="descContent"]/div/dl[1]/dd/cite[1]/text()').extract()
                    results['telephone'] = (''.join(telephone))[2:]
                if len(telephoneandfax) == 2:
                    telephone = sel.xpath('//*[@id="descContent"]/div/dl[1]/dd/cite[1]/text()').extract()
                    results['telephone'] = (''.join(telephone))[2:]
                    fax = sel.xpath('//*[@id="descContent"]/div/dl[1]/dd/cite[2]/text()').extract()
                    results['fax'] = (''.join(fax))[2:]
                    # print item["telephone"]
                    #  print item["fax"]
            if sel.xpath('//*[@id="js_hotelInfo_descBrief"]/p[2]'):  # 查看更多
                introduction = sel.xpath('//*[@id="js_hotelInfo_descAll"]/p/text()').extract()
                # item['introduction'] = ''.join(introduction)
                results['desc'] = ''.join(introduction)
            else:
                introduction = sel.xpath('//*[@id="descContent"]/div/dl[3]/dd/div/p/text()').extract()
                # item['introduction'] = ''.join(introduction)
                results['desc'] = ''.join(introduction)

            address = sel.xpath('//*[@id="detail_pageHeader"]/p/span[1]/@title').extract()

            # 酒店设施，包括（网络设施，停车场，酒店设施）

            supFac = ''
            # 网络设施
            inter_html = string_sub(response.body, inter_start, inter_end)
            if inter_html:
                sel_inter = Selector(text=inter_html)
                inter = sel_inter.xpath('//*[@class="each-facility inter-width"]/span/text()').extract()
                if inter:
                    # supFac += ''.join(inter) + '##'
                    results['has_wifi'] = '1'
            # 停车场
            stop_html = string_sub(response.body, stop_start, stop_end)
            if stop_html and len(stop_html):
                sel_stop = Selector(text=stop_html)
                stop = sel_stop.xpath('//*[@class="each-facility"]/span/text()').extract()
                if stop:
                    # supFac += ''.join(stop) + '##'
                    results['has_parking_lot'] = '1'
            # 酒店设施
            hotel_html = string_sub(response.body, hotel_start, hotel_end)

            supFac += icon_green(hotel_html)
            supFac = supFac[0:len(supFac) - 2]
            # 酒店服务
            service_html = string_sub(response.body, service_start, service_end)
            hotelSer = icon_green(service_html)
            if hotelSer and len(hotelSer) > 0:
                # item['hotelSer'] = hotelSer[0:len(hotelSer)-2]
                results['hotel_service'] = hotelSer[0:len(hotelSer) - 2]
            # 基本信息
            info = sel.xpath('//*[@id="descContent"]/div/dl[2]/dd/cite').extract()
            infolen = len(info)
            brandstr = ''
            for i in range(infolen):
                infoxpath = '//*[@id="descContent"]/div/dl[2]/dd/cite[' + str(i + 1) + ']/text()'
                infostr = sel.xpath(infoxpath).extract()[0]
                if '开业' in infostr:
                    results['opening_time'] = infostr
                elif '装修' in infostr:
                    results['decorate_time'] = infostr
                elif '客房' in infostr:
                    results['rooms'] = infostr
                elif '层' in infostr:
                    results['storeys'] = infostr
                else:
                    brandstr = brandstr + infostr + '##'
            results['brand'] = brandstr
            # if(info):
            # item['basicInfo'] = ''.join(info) #酒店的开业时间+酒店的装修时间+楼层数+房间数
            # item['type'] = '酒店'
            # item['dataSrc'] = 'qunar'
            # 房间设施
            room_html = string_sub(response.body, room_start, room_end)
            roomFac = icon_green(room_html)
            if (len(roomFac)):
                # item['roomFac'] = roomFac[0:len(roomFac) - 2]+supFac
                results['facility'] = roomFac[0:len(roomFac) - 2] + supFac

            cityName = response.meta['cityName']
            id = response.meta['id']
            # # 城市名称
            # item['city'] = cityName
            # yield item
            # 暂时不抓取周边交通信息（动态js加载）
            sur_tran_url = 'http://hotel.qunar.com/detail/detailMapData.jsp?seq=' + cityName + '_' + id + '&type=traffic%2Csubway'
            yield Request(sur_tran_url, meta={'item': item, "results": results}, callback=self.parse_sur_tran,
                          dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_sur_tran(self, response):
        try:
            results = response.meta['results']
            json_r = json.loads(response.body)

            # 解析经纬度，取谷歌的值
            mapposition = json_r["data"]["attrs"]["gpoint"]
            logstr = {"id": results["hotel_id"], "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      "data": json_r}
            self.logger_data.info(logstr)
            results["url"] = "http://hotel.qunar.com/city/" + results.get("city_name_gb") + "/dt-" + results[
                "hotel_id"] + "/"

            if len(mapposition) > 0:
                positionlist = mapposition.split(",")
                results["latitude"] = positionlist[0]  # response.meta['lat']#需要重新解析
                results["longitude"] = positionlist[1]  # response.meta['lng'] #需要重新解析
            traffic = json_r['data']['traffic']
            # 周边交通包括机场，火车站，汽车站，地铁站
            airport = ''
            airport_count = 0
            train = ''
            train_count = 0
            bus = ''
            bus_count = 0
            if len(traffic) > 0:
                for d in traffic:
                    category = int(d['category'])
                    name = d['name']
                    distance = float(d['distance']) / 1000
                    if category == 0:  # 机场
                        airport_count = airport_count + 1
                        airport += str(airport_count) + '##' + name + '##' + str(float('%0.1f' % distance)) + '公里$;'
                    elif category == 1:  # 火车站
                        train_count = train_count + 1
                        train += str(train_count) + '##' + name + '##' + str(float('%0.1f' % distance)) + '公里$;'
                    elif category == 2:  # 汽车站
                        bus_count = bus_count + 1
                        bus += str(bus_count) + '##' + name + '##' + str(float('%0.1f' % distance)) + '公里$;'
            subway_data = json_r['data']['subway']
            subway = ''
            subway_count = 0
            if len(subway_data) > 0:
                for s in subway_data:
                    name = s['name']
                    distance = float(s['distance']) / 1000
                    subway_count = subway_count + 1
                    subway += str(subway_count) + '##' + name + '##' + str(float('%0.1f' % distance)) + '公里$;'
                # surTra = []
                if (len(airport)):
                    # surTra.append(['airport', airport[0:len(airport)-2]])
                    results['surround_airport'] = airport[0:len(airport) - 2]
                    # if(len(train)):
                    # surTra.append(['train', train[0:len(train)-2]])
                if (len(bus)):
                    # surTra.append(['bus', bus[0:len(bus)-2]])
                    results['surround_bus_station'] = bus[0:len(bus) - 2]
                if (len(subway)):
                    results['surround_metro'] = train[0:len(train) - 2]
                    # surTra.append(['subway', subway[0:len(subway)-2]])
                    #  item['surTra'] = surTra
                    # log.msg("The hotel Info item:::"+str(item),loglevel=log.INFO)
                    # self.qunarlogger.info(item)
            score_url = "http://review.qunar.com/api/h/" + results["city_name_gb"] + "_" + results[
                "hotel_id"] + "/v2/detail?__jscallback=jQuery1830501302233134745_1467714492396"
            yield Request(score_url, meta={'item': response.meta['item'], "results": results},
                          callback=self.parse_score, dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['item']['results']['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_score(self, response):
        try:
            results = response.meta['results']
            resp_str = response.body
            if resp_str:
                resp_str_len = len(resp_str)
                json_r = json.loads(resp_str[40:resp_str_len - 1])
                logstr = {"id": results["hotel_id"], "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          "data": json_r}
                self.logger_data.info(logstr)
                if 'hotelScore' and 'countStat' and 'itemList' in json_r['data'].keys():
                    results['score'] = json_r.get('data').get('hotelScore')
                    commcnt1 = json_r.get('data').get('countStat').get('commCnt')
                    commcnt2 = json_r.get('data').get('countStat').get('guruCnt')
                    commcnt = commcnt1 + commcnt2
                    #图片评论
                    imgcont = json_r.get('data').get('countStat').get("imgCnt")
                    results['total_comment_count'] = u'总评论' + str(commcnt)
                    itemlist = json_r.get('data').get('itemList')
                #
                # str1 = ""
                # for il in itemlist:
                #     name = str(il.get("name"))
                #     score = str(il.get("score"))
                #     str1 = str1 + " " + name + ":" + score + " "
                # results["assess_comment"] = str1
                    if itemlist:
                        results["avg_position_score"] = itemlist[3].get("score")
                        results["avg_facility_score"] = itemlist[0].get("score")
                        results["avg_health_score"] = itemlist[1].get("score")
                        results["avg_service_score"] = itemlist[2].get("score")
                        results["cost_effective_score"] = itemlist[5].get("score")
                        results["avg_food_score"] = itemlist[4].get("score")

                #  yield item
            sur_hotel_url = "http://hotel.qunar.com/render/detailRecommend.jsp?hotelSEQ=" + results["city_name_gb"] + "_" + str(results["hotel_id"]) + "&startTime=" + (
                            datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + "&endTime=" + (
                            datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d')
            yield Request(sur_hotel_url, meta={'item': response.meta['item'], "results": results},
                          callback=self.parse_surhotel, dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['item']['results']['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_surhotel(self, response):
        try:
            item = response.meta['item']
            results = response.meta['results']
            resp_body = response.body
            json_respone = json.loads(resp_body.encode("utf-8").decode("utf-8", "ignore"))
            logstr = {"id": results["hotel_id"], "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                      "data": json_respone}
            self.logger_data.info(logstr)
            sur_hotel = []
            keys = json_respone["data"]["groupHotels"].keys()
            groupHotels = json_respone["data"]["groupHotels"]
            # print keys
            i = 1
            for key in keys:
                hotel_info = ''
                hotel_info = hotel_info + str(i) + groupHotels[key]["hotelName"]
                if 'stars' in groupHotels[key].keys():
                    hotel_info = hotel_info + "##" + groupHotels[key]["stars"] + "星级"
                if 'dangci' in groupHotels[key].keys():
                    hotel_info = hotel_info + "##档次" + str(groupHotels[key]["dangci"])
                if 'distance' in groupHotels[key].keys():
                    hotel_info = hotel_info + "##距离" + str(groupHotels[key]["distance"]) + "千米"
                if 'marketPrice' in groupHotels[key].keys():
                    hotel_info = hotel_info + "##" + str(groupHotels[key]["marketPrice"]) + "起;"
                else:
                    hotel_info = hotel_info + ";"
                sur_hotel.append(hotel_info)
                i = i + 1
            results["surround_hotel"] = sur_hotel
            url = "http://te.review.qunar.com/api/h/" + results["city_name_gb"] + "_" + str(
                results["hotel_id"])  + "/detail/rank/v1/page/1" + \
                  "?u2=null&__jscallback=jQuery183007184270187281072_1473421957598&rate=all&onlyGuru=false"
            yield Request(url=url, meta={'item': response.meta['item'], "results": results,
                                    }, callback=self.parse_comment,
                      dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['item']['results']['hotel_id']
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
    # 解析好评、中评、差评
    def parse_comment(self, response):
        try:
            resp_str = response.body
            item = response.meta['item']
            results = response.meta['results']
            if resp_str :
                body = resp_str[42:len(resp_str) - 2]
                json_r = json.loads(body)
                total = json_r.get("data").get("count")
                results["total_comment_count"] = u'总评论' + str(total)
                list = json_r.get("data").get("ratingStat")
                results["bad_comment_count"] = list.get("negativeCount")
                results["general_comment_count"] = list.get("neutralCount")
                good_comment = list.get("positiveCount")
                results["good_comment_count"] = good_comment
                results["good_comment_rate"] = round(float(good_comment) / total, 2)

            item['results'] = results
            print item
            yield item
            # self.logger_data.info(item)

            # t = time.time()
            # url = 'http://hotel.qunar.com/render/hotelLastMsg.jsp?hotelseq=' + results["city_name_gb"] + "_" + str(
            #     results["hotel_id"]) + '&t=' + str(t) + ''
            # yield Request(url, meta={'item': response.meta['item'], "results": results},
            #               callback=self.parse_latest_book, dont_filter=True)
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.QUNAR
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['item']['results']['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))


    # def parse_latest_book(self, response):
    #     try:
    #         item = response.meta['item']
    #         results = response.meta['results']
    #         resp_body = json.loads(response.body)
    #         btime = resp_body.get('data').get('bookingTime')
    #         onlineCustomer = resp_body.get('data').get('onlineCustomer')
    #         if (btime):
    #             results["latest_book"] = btime + "前有人预订了该酒店"
    #         results["url"] = "http://hotel.qunar.com/city/" + results.get("city_name_gb") + "/dt-" + results[
    #             "hotel_id"] + "/"
    #         results["hotel_id"] = results["city_name_gb"] + "_" + results["hotel_id"]  # 统一字段
    #         item['results'] = results
    #         print item
    #         self.logger_data.info(item)
    #
    #     except :
    #         error_log_dic = {}
    #         error_log_dic['channel_id'] = ChannelEnum.QUNAR
    #         error_log_dic['platform_id'] = PlatformEnum.PC
    #         error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
    #         error_log_dic['id'] = response.meta['item']['results']['hotel_id']
    #         error_log_dic['pid'] = ''
    #         error_log_dic['error_info'] = traceback.format_exc()
    #         error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
    #         error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #         self.logger_error.error(json.dumps(error_log_dic))

# 截取字符串
def string_sub(text, startTag, endTag):
    if text:
        start = text.find(startTag)
        end = text.find(endTag)
        if start != -1 and end != -1:
            return text[start + len(startTag):end]


# 获取绿色icon的内容
def icon_green(html):
    result = ''
    green = 'icon-correct green'
    sel_hotel = Selector(text=html)
    dr = re.compile(r'<[^>]+>|(&#xe621;)|(\s+)', re.S)
    green_re = re.compile(r'<i class="icon-correct green">(.*)</i>')
    hotel = sel_hotel.xpath('//*[@class="each-facility"]')
    for h in hotel:
        class_i = h.xpath('span/i/@class').extract()
        if green == ''.join(class_i):
            text_green = ''.join(h.xpath('span').extract())
            text_green = green_re.sub('', text_green)
            result += dr.sub('', text_green) + '##'
    return result
