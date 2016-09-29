#coding=utf-8
import sys
import traceback

from ycfspider.utils.enum import *

reload(sys)
sys.setdefaultencoding("utf-8")

import requests
import time
import random
import os
from ycfspider.items import YcfspiderItem
import json
from scrapy.http import Request
from lxml import etree
from scrapy_redis.spiders import RedisSpider
from ycfspider.utils.model_logger import Logger
import re,datetime
from scrapy.selector import Selector
from scrapy.conf import settings
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DontCloseSpider
from ycfspider.utils.redisutils import RedisUtil
from ycfspider.utils.useragent import user_agent_list
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord

class CtripPcHotelInfoSpider(RedisSpider):
    name = "CtripPcHotelInfoSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["hotels.ctrip.com"]
    start_urls = []
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:ctrip_pc_hotel_info_all'
    else:
        redis_key = 'spider:ctrip_pc_hotel_info_ycf'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name+':requests')
    settings = get_project_settings()
    header = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate, sdch",
        "Accept-Language":"zh-CN,zh;q=0.8",
        "Cache-Control":"max-age=0",
        "Connection":"keep-alive",
        "Cookie":"_abtest_userid=fc1616d8-db27-4209-914c-301ac78e4b1e; Session=SmartLinkCode=U155950&SmartLinkKeyWord=&SmartLinkQuary=&SmartLinkHost=&SmartLinkLanguage=zh; HotelCityID=1split%E5%8C%97%E4%BA%ACsplitBeijingsplit2016-7-27split2016-07-28split0; Union=OUID=000401app-&AllianceID=4897&SID=155950&SourceID=; _bfa=1.1469513292941.3ru1z2.1.1469612486497.1469755342859.8.54; _bfs=1.1; HotelDomesticVisitedHotels1=827426=0,0,0,5,/hotel/826000/825607/51de7b4f2c9d40dfaa2aa675408e6acc.jpg,&1225627=0,0,4.7,2055,/t1/hotel/953/418/306/1d9c30823d6e44be95e58281eabe1ad9.jpg,&1545931=0,0,4.2,190,/fd/hotel/g1/M09/56/39/CghzflWfU-2AeY2XAAWW5Ad873A477.jpg,&469876=0,0,4.2,3939,/hotel/82000/81674/6bd5633bc95648ecae0de2c34d800a85.jpg,&1405969=0,0,4.6,1798,/t1/hotel/6000/5698/74a68c8a286a49d693b17919a5d17450.jpg,&691682=0,0,4.3,5658,/fd/hotel/g4/M08/12/01/CggYHFXepUuAJmRUAAqGdhCANDQ642.jpg,; __zpspc=9.8.1469755346.1469755346.1%234%7C%7C%7C%7C%7C%23; _jzqco=%7C%7C%7C%7C%7C1.205345167.1469513295017.1469614631618.1469755346312.1469614631618.1469755346312.0.0.0.41.41; appFloatCnt=5; _ga=GA1.2.1627737920.1469513295; _gat=1; manualclose=1; _bfi=p1%3D102003%26p2%3D0%26v1%3D54%26v2%3D0",
        "Host":"hotels.ctrip.com",
        "Upgrade-Insecure-Requests":1,
        "User-Agent": random.choice(user_agent_list)
    }

    def __init__(self, *args, **kwargs):
        super(CtripPcHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')
        # spiderStateRecord.flag_record(self.name)

    def spider_idle(self):
        self.schedule_next_request()
        if settings.get('LOOP', False) or self.server.llen(self.redis_key):
            raise DontCloseSpider

    def next_request(self):
        if self.server.llen(self.name+':requests')==0:
            item = self.server.lpop(self.redis_key)
            if item:
                item = eval(item)
                if self.redis_key=='spider:ctrip_pc_hotel_info_all':
                    return Request(item["url"], meta={'hotel_id':item["hotel_id"]},callback=self.parse,dont_filter=True,headers=self.header)
                if self.redis_key=='spider:ctrip_pc_hotel_info_ycf':
                    url = 'http://hotels.ctrip.com/hotel/'+item['hotel_id']+'.html'
                    return Request(url, meta={'hotel_id':item["hotel_id"]},callback=self.parse,dont_filter=True,headers=self.header)
    def parse(self,response):
        try:
            results = {"id":"","desc":"","retail_price":"","hotel_id":"","hotel_name":"","hotel_used_name":"","address":"","short_address":"","channel_id":"",
                        "hotel_belongs":'0',"country_code":"CN","country_name":'china',"province_code":"","province_name":"","city_code":"","city_name_cn":"","city_name_gb":"",
                         "district_code":"","district_name":"","postal_code":"","business":"","position_type":u'经纬度',"longitude":"","latitude":"","telephone":"",
                          "email":"","fax":"","url":"","picture_url":"","picture_list_url":"","comment_desc":"","brand":"","type":"","level":"","score":"","total_comment_count":"",
                          "good_comment_rate":"","favourite_count":"","praise_count":"","share_count":"","opening_time":"","decorate_time":"","storeys":"","rooms":"",
                          "policy":"","check_in_time":"","check_out_time":"","is_pet_allowed":"","is_credit_card_supportted":"","facility":"","hotel_service":"","has_parking_lot":"",
                          "has_wifi":"","has_atm":"","has_restaurant":"","has_gym":"","shared_url":"","active_facility":"","status":"","hotel_tag":"","area_tag":"","remark":"",
                          "keywords":"","crawl_version":"","crawl_time":"","surround_airport":"","surround_metro":"","surround_bus_station":"","surround_food":"","surround_entertainment":"",
                          "surround_rent_car":"","surround_scenic":"","surround_shopping":"","surround_hotel":"","platform_id":"1","reserved_col1":"","reserved_col2":"","reserved_col3":"","reserved_col4":"",
                          "reserved_col5":""}

            results["hotel_id"] = response.meta['hotel_id']
            time.sleep(0.5)
            content = response.body
            logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":content}
            self.logger_data.info(json.dumps(logstr))
            if content:
                contentTree = etree.HTML(content, etree.HTMLParser(encoding="utf-8"))
                city_name_gb = contentTree.xpath('//input[@id="cityPY"]/@value')
                if city_name_gb:
                    results['city_name_gb'] = city_name_gb[0]
                hotel_name = contentTree.xpath('//h2[@class="cn_n"]')
                if len(hotel_name)>0:
                  results['hotel_name'] = hotel_name[0].text#("//h2[@itemprop='name']")[0].text
                results['channel_id']='1'
                results['hotel_belongs'] = '0'
                results['country_name'] = 'China'
                results['province_code'] = ''
                pattern1 = re.compile("province=(.*?);city", re.S)
                province_name = re.findall(pattern1, content)
                if (len(province_name) > 0):
                    results['province_name'] = province_name[0]
                else:
                    results['province_name'] = ''
                pattern1 = re.compile("cityId:'(\d+)',\s", re.S)
                cityId = re.findall(pattern1, content)
                if len(cityId):
                   results['city_code'] = int(cityId[0])
                else:
                    results['city_code']=''
                city_name_cn = contentTree.xpath("//div[@id='searchForm']/input[1]/@value")
                if len(city_name_cn)>0:
                     results['city_name_cn'] = city_name_cn[0]
                results['short_address'] = ''
                results["retail_price"]=''
                address = contentTree.xpath("//div[@id='J_htl_info']/div[@class='adress']")
                if len(address)>0:
                    results['address'] = address[0].xpath("string(.)").replace(' ', '').replace('\n', '')
                pattern = re.compile("hotellat: '(.*?)',\s", re.S)
                hotellat = re.findall(pattern, content)
                pattern = re.compile("hotellon: '(.*?)',\s", re.S)
                hotellon = re.findall(pattern, content)
                if hotellon:
                    results['longitude'] = str(hotellon[0])
                if hotellat:
                    results['latitude'] = str(hotellat[0])
                tel = contentTree.xpath("//div[@id='htlDes']/p/span//@data-real")
                if len(tel)>0:
                    tel = tel[0]
                    if (tel[3] == '-'):
                        results['telephone'] = tel[4:15]
                    elif (tel[2] == '1'):
                        results['telephone'] = tel[2:13]
                    else:
                        if (tel[14].isdigit()):
                            results['telephone'] = tel[2:15]
                        else:
                            results['telephone'] = tel[2:14]
                    results['email'] = ''
                    fax = tel.split("传真")
                    if (len(fax) > 1):
                        faxstr = fax[1]
                        for i in range(0, len(fax[1])):
                            if (faxstr[i].isdigit() or faxstr[i] == '-'):
                                pass
                            else:
                                break
                        fax = faxstr[0:i]
                        results['fax'] = fax
                    else:
                        results['fax'] = ''
                results['url'] = "http://hotels.ctrip.com/hotel/" + response.meta['hotel_id'] + ".html"
                picture_list = contentTree.xpath("//div[@id='topPicList']/div/div//@_src")
                if(len(picture_list)==0):
                    results['picture_url']=''
                    results['picture_list_url']=''
                else:
                    results['picture_url'] = picture_list[0]
                    picture_list_url=''

                    for p in range(1, len(picture_list)):
                         picture_list_url = picture_list_url + picture_list[p] + '##'

                    if (len(picture_list_url) >= 2):
                        picture_list_url = picture_list_url[0:len(picture_list_url)-2]
                    results['picture_list_url'] = picture_list_url
                desc = contentTree.xpath("//span[@itemprop='description']")
                if len(desc)>0:
                    results['desc'] = desc[0].xpath("string(.)").replace(' ','').replace('\n', '').strip()
                title_num = contentTree.xpath("//div[@id='base_bd']/div[@class='path_bar2']/a")
                if (len(title_num) == 4):
                    results['brand'] = contentTree.xpath("//div[@id='base_bd']/div[@class='path_bar2']/a[3]")[0].text
                    results['district_name'] = contentTree.xpath("//div[@id='base_bd']/div[@class='path_bar2']/a[2]")[0].text
                else:
                    results['brand'] = ''
                    results['district_name'] = ''
                results['type'] = ''
                level = contentTree.xpath("//div[@class='grade']/span//@title")
                if len(level) != 0:
                    results['level'] = level[0]
                else:
                    results['level'] = ""
                score = contentTree.xpath("//a[@class='commnet_score']/p[@class='s_row']/span")
                if (len(score) == 0):
                    results['score'] = ''
                else:
                    results['score'] = score[0].text
                total_comment_count = \
                contentTree.xpath("//span[@class='commnet_num']/span[@itemprop='reviewCount']")
                if(len(total_comment_count)==0):
                    results['total_comment_count']=''
                else:
                    results['total_comment_count']=total_comment_count[0].text.split("位")[0]
                basicdesc = contentTree.xpath("//div[@id='htlDes']/p")
                if len(basicdesc)>0:
                    basicdesc = basicdesc[0].text
                    opening_time = decorate_time = rooms = ''
                    if (basicdesc.find('开业') != -1):
                        opening = basicdesc.split('开业')
                        opening_time = opening[0]
                        basicdesc = opening[1]
                    if (basicdesc.find('装修') != -1):
                        decorate = basicdesc.split('装修')
                        decorate_time = decorate[0].replace('  ', '')
                        basicdesc = decorate[1]
                    if (basicdesc.find('间房') != -1):
                        room = basicdesc.split('间房')
                        rooms = room[0].replace('  ', '')
                    results['opening_time'] = opening_time
                    results['decorate_time'] = decorate_time
                    results['rooms'] = rooms
                    results['storeys'] = ''
                good_comment_rate = contentTree.xpath("//a[@class='commnet_score']/p[2]")
                if(len(good_comment_rate)==0):
                    results['good_comment_rate']=''
                else:
                    results['good_comment_rate']=good_comment_rate[0].text
                results['favourite_count'] = None

                url = "http://m.ctrip.com/restapi/soa2/10935/hotel/booking/commentgroupsearch"
                req_header = {
                    'Host': 'm.ctrip.com',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
                }
                values = {"flag": 1, "id": results['hotel_id'], "htype": 1, "sort": {"idx": 1, "size": 10, "sort": 2, "ord": 1},
                          "search": {"kword": "", "gtype"
                          : 54, "opr": 1, "ctrl": 14, "filters": []},
                          "alliance": {"aid": "66672", "sid": "508668", "ouid": "", "ishybrid": 0}
                    , "head": {"cid": "09031087110286103415", "ctok": "", "cver": "1.0", "lang": "01", "sid": "55552328",
                               "syscode": "09"
                        , "auth": None, "extension": [{"name": "pageid", "value": "228032"}, {"name": "webp", "value": 0},
                                                      {"name": "protocal"
                                                          , "value": "http"}]}, "contentType": "json"}

                _session = requests.Session()
                _session.headers.update(req_header)
                json_post_values = json.dumps(values)
                res = _session.post(url, data=json_post_values)
                json_get_values = json.loads(res.content)

                if 'hcdi' in json_get_values:
                    hcdi = json_get_values['hcdi']
                    if 'stats' in hcdi:
                        stats = hcdi['stats']
                        if 'rcmc' in stats:
                            results['praise_count']=str(stats['rcmc'])
                        else:
                            results['praise_count']=''
                    else:
                        results['praise_count']=''
                else:
                    results['praise_count']=''

                comment_desc = ''
                if 'hcsi' in json_get_values:
                    hcsi = json_get_values['hcsi']
                    if 'avgpts' in hcsi:
                        avgpts = hcsi['avgpts']
                        if 'arnd' in avgpts:
                            comment_desc = comment_desc + '位置' + str(avgpts['arnd']) + '##'
                        if 'facl' in avgpts:
                            comment_desc = comment_desc + '设施' + str(avgpts['facl']) + '##'
                        if 'serv' in avgpts:
                            comment_desc = comment_desc + '服务' + str(avgpts['serv']) + '##'
                        if 'room' in avgpts:
                            comment_desc = comment_desc + '卫生' + str(avgpts['room']) + '##'

                if 'hcdi' in json_get_values:
                    hcdi = json_get_values['hcdi']
                    if 'csqs' in hcdi:
                        csqs = hcdi['csqs']
                        if (len(csqs) > 0):
                            if 'items' in csqs[0]:
                                impress = csqs[0]['items']
                                for i in range(0, len(impress)):
                                    comment_desc = comment_desc + impress[i]['name'] + '(' + str(impress[i]['val']) + ')' + '##'
                if 'hcdi' in json_get_values:
                    hcdi = json_get_values['hcdi']
                    if 'stats' in hcdi:
                        stats = hcdi['stats']
                        if 'ttlc' in stats:
                            comment_desc = comment_desc + '全部' + '(' + str(stats['ttlc']) + ')' + '##'
                        if 'rcmc' in stats:
                            comment_desc = comment_desc + '值得推荐' + '(' + str(stats['rcmc']) + ')' + '##'
                            if 'nrcc' in stats:
                                comment_desc = comment_desc + '有待改善' + '(' + str(stats['nrcc']) + ')' + '##'
                        if 'imgc' in stats:
                            comment_desc = comment_desc + '有图片' + '(' + str(stats['imgc']) + ')' + '##'
                if (len(comment_desc) >= 2):
                    comment_desc = comment_desc[0:len(comment_desc) - 2]
                results['comment_desc']=comment_desc
                basicInfo = ''
                hotelpols = contentTree.xpath("//div[@class='htl_info_table']/table[@class='detail_extracontent']/tr/th")
                for i in range(0, len(hotelpols) - 1):
                    hotelpolsd = contentTree.xpath("//div[@class='htl_info_table']/table[@class='detail_extracontent']/tr/td")[
                        i].xpath("string(.)")
                    basicInfo = basicInfo + hotelpols[i].text + hotelpolsd + "#"
                basicInfo = basicInfo.replace('\n', '').replace(' ', '').replace('\t', '')
                results['policy'] = basicInfo
                if (basicInfo.find('不可携带宠物') == -1):
                    results['is_pet_allowed'] = 1
                else:
                    results['is_pet_allowed'] = 0
                credit = contentTree.xpath(
                    "//div[@class='htl_info_table']/table[@class='detail_extracontent']/tr[last()]/td/div/span[1]//@data-params")
                if (len(credit) == 0):
                    results['is_credit_card_supportted'] = 0
                else:
                    if (credit[0].find('卡') == -1):
                        results['is_credit_card_supportted'] = 0
                    else:
                        results['is_credit_card_supportted'] = 1
                pattern1 = re.compile("checkIn:'(.*?)',\s", re.S)
                checkIn = re.findall(pattern1, content)
                pattern1 = re.compile("checkOut:'(.*?)'\s", re.S)
                checkOut = re.findall(pattern1, content)
                if checkIn:
                    results['check_in_time'] = checkIn[0]
                if checkOut:
                    results['check_out_time'] = checkOut[0]

                num = contentTree.xpath("//div[@id='J_htl_facilities']/table/tbody/tr")
                facility = hotel_service = active_facility = ""

                for n in range(1, len(num)):
                    fac_path = "//div[@id='J_htl_facilities']/table/tbody/tr[{0}]/th".format(n)
                    fac_title = contentTree.xpath(fac_path)[0].text
                    if (fac_title == '通用设施' or fac_title == '客房设施'):
                        fac_detail = contentTree.xpath(
                            "//div[@id='J_htl_facilities']/table/tbody/tr[{0}]/td/ul/li//@title".format(n))
                        for f in fac_detail:
                            facility = facility + f + "##"

                    elif (fac_title == '服务项目'):
                        fac_detail = contentTree.xpath(
                            "//div[@id='J_htl_facilities']/table/tbody/tr[{0}]/td/ul/li//@title".format(n))
                        for f in fac_detail:
                            hotel_service = hotel_service + f + "##"

                    elif (fac_title == '活动设施'):
                        fac_detail = contentTree.xpath(
                            "//div[@id='J_htl_facilities']/table/tbody/tr[{0}]/td/ul/li//@title".format(n))
                        for f in fac_detail:
                            active_facility = active_facility + f + "##"

                if (len(facility) >= 2):
                    facility = facility[0:len(facility) - 2]
                if (len(hotel_service) >= 2):
                    hotel_service = hotel_service[0:len(facility) - 2]
                if (len(active_facility) >= 2):
                    active_facility = active_facility[0:len(facility) - 2]

                results['facility'] = facility
                results['hotel_service'] = hotel_service
                results['active_facility'] = active_facility

                if (facility.find('停车场') == -1):
                    results['has_parking_lot'] ='0'
                else:
                    results['has_parking_lot'] = '1'
                if (facility.find('有可无线上网的公共区域') == -1):
                    results['has_wifi'] = '0'
                else:
                    results['has_wifi'] ='1'
                #results['has_atm'] = 0
                if (facility.find('餐厅') == -1):
                    results['has_restaurant'] = '0'
                else:
                    results['has_restaurant'] = '1'
                if (active_facility.find('健身') == -1):
                    results['has_gym'] = '0'
                else:
                    results['has_gym'] = '1'
                results['shared_url'] = ''

                results['surround_airport'] = ''
                results['surround_bus_station'] = ''
                results['surround_rent_car'] = ''
                results['surround_hotel'] = ''

                surround_metro = surround_scenic = surround_shopping = surround_food = surround_entertainment = ''
                surround_num = contentTree.xpath("//div[@class='htl_info_table']/table/tbody/tr")
                if (len(surround_num) > 0):
                    for s in range(0, len(surround_num)):
                        surround_name = \
                        contentTree.xpath("//div[@class='htl_info_table']/table/tbody/tr[{0}]/th".format(s + 1))[0].text
                        if (surround_name == '餐饮'):
                            surround_info = contentTree.xpath(
                                "//div[@class='htl_info_table']/table/tbody/tr[{0}]/td/ul/li".format(s + 1))
                            for i in surround_info:
                                surround_food = surround_food + i.text + "##"
                        elif (surround_name == '购物'):
                            surround_info = contentTree.xpath(
                                "//div[@class='htl_info_table']/table/tbody/tr[{0}]/td/ul/li".format(s + 1))
                            for i in surround_info:
                                surround_shopping = surround_shopping + i.text + "##"
                        elif (surround_name == '娱乐'):
                            surround_info = contentTree.xpath(
                                "//div[@class='htl_info_table']/table/tbody/tr[{0}]/td/ul/li".format(s + 1))
                            for i in surround_info:
                                surround_entertainment = surround_entertainment + i.text + "##"
                        elif (surround_name == '地铁站'):
                            surround_info = contentTree.xpath(
                                "//div[@class='htl_info_table']/table/tbody/tr[{0}]/td/ul/li".format(s + 1))
                            for i in surround_info:
                                surround_metro = surround_metro + i.text + "##"
                        elif (surround_name == '景点'):
                            surround_info = contentTree.xpath(
                                "//div[@class='htl_info_table']/table/tbody/tr[{0}]/td/ul/li".format(s + 1))
                            for i in surround_info:
                                surround_scenic = surround_scenic + i.text + "##"
                if (len(surround_food) >= 2):
                    surround_food = surround_food[0:len(surround_food) - 2]
                if (len(surround_shopping) >= 2):
                    surround_shopping = surround_shopping[0:len(surround_shopping) - 2]
                if (len(surround_entertainment) >= 2):
                    surround_entertainment = surround_entertainment[0:len(surround_entertainment) - 2]
                if (len(surround_metro) >= 2):
                    surround_metro = surround_metro[0:len(surround_metro) - 2]
                if (len(surround_scenic) >= 2):
                    surround_scenic = surround_scenic[0:len(surround_scenic) - 2]
                results['surround_food'] = surround_food
                results['surround_shopping'] = surround_shopping
                results['surround_entertainment'] = surround_entertainment
                results['surround_metro'] = surround_metro
                results['surround_scenic'] = surround_scenic

                tags = contentTree.xpath("//div[@class='special_label']/i")
                tagstr = ""
                for tag in tags:
                    tagstr += tag.text
                    tagstr += "##"
                if len(tagstr) >= 2:
                    tagstr = tagstr[0:len(tagstr) - 2]
                results['hotel_tag'] = tagstr
                results['area_tag'] = ''
                results['remark'] = ''
                results['keywords'] = ''
                results['crawl_version'] = ''
                results['crawl_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
               # item["results"] = results
               #  add_info_xpath = '//*[@language="javascript"]/text()'

                if results["latitude"] and results["longitude"]:
                    url = 'http://hotels.ctrip.com/Domestic/tool/AjaxGetHotelRelationInfo.aspx?city='+str(results["city_code"])+"&hotel="+\
                          str(results["hotel_id"])+"&EDM=&startdate="+str(datetime.datetime.now().strftime('%Y-%m-%d'))+"&enddate="+\
                          str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))+"&lat="+str(results["latitude"])+"&lot="+str(results["longitude"])+"&isghi=F"
                    yield Request(url,meta={"results":results},callback=self.parse_sur_hotel,dont_filter=True,headers=self.header)
                else:
                    item = YcfspiderItem()
                    item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_INFO_RESOURSE_PATH')
                    item["results"] = results
                    yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['hotel_id']
            error_log_dic['pid'] = ''
            error_log_dic['error_info'] = traceback.format_exc()
            error_log_dic['error_type'] = ErrorTypeEnum.PARSEERROR
            error_log_dic['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger_error.error(json.dumps(error_log_dic))

    def parse_sur_hotel(self,response):
        try:
            sel = Selector(response)
            results = response.meta['results']
            logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":response.body}
            self.logger_data.info(json.dumps(logstr))
            li_xpath = '//*[@id="around_same_type_hotel"]/li'
            li = sel.xpath(li_xpath).extract()
            if li:
                surround_hotel = ''
                for i in range(len(li)):
                    distance_xpath = li_xpath+'['+str(i+1)+']/span[@class="d also_viewed"]/strong/text()'
                    distance = sel.xpath(distance_xpath).extract()
                    if distance:
                        distance = distance[0]
                    else:
                        distance = 0
                    name_xpath =  li_xpath+'['+str(i+1)+']/div[@class="name"]/a/text()'
                    name = sel.xpath(name_xpath).extract()
                    if name:
                        name = name[0]
                    else:
                        name = ''
                    price_xpath =  li_xpath+'['+str(i+1)+']/div[@class="price"]/strong/text()'
                    price = sel.xpath(price_xpath).extract()
                    if price:
                        price = price[0]
                    else:
                        price = 0
                    surround_hotel = surround_hotel + str(i+1) +';'+ name+';距离'+ str(distance)+'km;￥'+str(price)+'元起##'
                results["surround_hotel"] = surround_hotel
            item = YcfspiderItem()
            item["kafka_url"] = settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_INFO_RESOURSE_PATH')
            item["results"] = results
            yield item
        except:
            error_log_dic = {}
            error_log_dic['channel_id'] = ChannelEnum.CTRIP
            error_log_dic['platform_id'] = PlatformEnum.PC
            error_log_dic['type_id'] = CrawlTypeEnum.HOTELINFO
            error_log_dic['id'] = response.meta['results']['hotel_id']
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