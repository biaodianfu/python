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
from scrapy import log
# from ycfspider.utils.spider_state_flag_record import spiderStateRecord


class ElongPcHotelInfoSpider(RedisSpider):
    name = "ElongPcHotelInfoSpider"
    filename = settings.get('LOG_PATH')+'/' + name
    if not os.path.exists(filename):
            os.makedirs(filename)
    allowed_domains = ["hotel.elong.com"]
    start_urls = []
    if settings.get('SCALE') == 'all':
        redis_key = 'spider:elong_pc_hotel_info_all'
    else:
        redis_key = 'spider:elong_pc_hotel_info_ycf'
    if not settings.get('PERSIST_REQUESTS'):
        if settings.get('SCALE') == 'all':
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=0)
        else:
            r = RedisUtil(host=settings.get('REDIS_HOST'), db=1)
        r.delete(name + ':requests')

    def __init__(self, *args, **kwargs):
        super(ElongPcHotelInfoSpider, self).__init__(*args, **kwargs)
        self.logger_data = Logger(settings.get('LOG_PATH')+'/' + self.name + '/original/')
        self.logger_error = Logger(settings.get('LOG_PATH')+'/' + self.name + '/error/')

    def next_request(self):
        item = self.server.lpop(self.redis_key)
        if item:
            item = eval(item)
            urlstr = 'http://hotel.elong.com/'+item["hotel_id"]+'/'
            meta = {'cityId': item["city_id"], 'hotelname': item["hotel_name"],
                    'lat': item["lat"], 'lng': item["lng"], 'hoteladdress': item["hotel_address"],
                    'cityNameEn': item["city_name_gb"], 'hotelid': item["hotel_id"]}
            return Request(urlstr,meta = meta,dont_filter=True,callback=self.parse,headers=self.header1)

    def parse(self, response):
        item = YcfspiderItem()
        item["kafka_url"] =  settings.get('KAFKA_ADDRESS') + settings.get('KAFKA_HOTEL_PRICE_RESOURSE_PATH')
        results = {"id":"","desc":"","retail_price":"","hotel_id":response.meta['hotelid'],"hotel_name":response.meta['hotelname'],"hotel_used_name":"","address":response.meta['hoteladdress'],"short_address":"","channel_id":"3",
                    "hotel_belongs":'0',"country_code":"CN","country_name":'china',"province_code":"","province_name":"","city_code":"","city_name_cn":"","city_name_gb":response.meta['cityNameEn'],
                     "district_code":"","district_name":"","postal_code":"","business":"","position_type":u"经纬度","longitude":response.meta['lng'],"latitude":response.meta['lat'],"telephone":"",
                      "email":"","fax":"","url":'http://hotel.elong.com/'+response.meta['cityNameEn']+'/'+response.meta['hotelid']+'/',"picture_url":"","picture_list_url":"","comment_desc":"","brand":"","type":"","level":"","score":"","total_comment_count":"",
                      "good_comment_rate":"","favourite_count":"","praise_count":"","share_count":"","opening_time":"","decorate_time":"","storeys":"","rooms":"",
                      "policy":"","check_in_time":"","check_out_time":"","is_pet_allowed":"","is_credit_card_supportted":"","facility":"","hotel_service":"","has_parking_lot":"",
                      "has_wifi":"","has_atm":"","has_restaurant":"","has_gym":"","shared_url":"","active_facility":"","status":"","hotel_tag":"","area_tag":"","remark":"",
                      "keywords":"","crawl_version":"","crawl_time":time.strftime("%Y-%m-%d %H:%M:%S"),"surround_airport":"","surround_metro":"","surround_bus_station":"","surround_food":"","surround_entertainment":"",
                      "surround_rent_car":"","surround_scenic":"","surround_shopping":"","surround_hotel":"","platform_id":"1","reserved_col1":"","reserved_col2":"","reserved_col3":"","reserved_col4":"",
                      "reserved_col5":"","latest_book" : "","gassess" : "","massess":"","bassess":"","trip_advisor_assess":"","assess_comment":"","room_assess":"","content_assess":[]}

        hotelStr = response.body
        hotellogstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":hotelStr}
        self.elong_pc_hotel_info_logger.info(hotellogstr)
        # now = time.strftime("%Y-%m-%d %H:%M:%S")
        # unixnow = util.datetime_timestamp(now)
        # path = self.settings["FILEPATH"]
       # textstr =path +"elong_data/"+response.meta['cityNameEn'] + '_'+response.meta['hotelname']+str(unixnow)+'.txt'
        #print '--------file----------'
       # print textstr
       # fx = open(textstr,'a')
        #fx.write(hotelStr)
        #fx.close()
        soup = BeautifulSoup(hotelStr,"lxml")                                                                          #开始解析
        #解析praise_count
        praise_count_soup = soup.find(attrs={"class":"hrela_comt bbddd hrela_add"}).findAll("div")
        if len(praise_count_soup)>0:
            results["praise_count"] = re.findall(r'[0-9]+',praise_count_soup[2].findAll("p")[0].text)[0]
        if soup.find(attrs={"id":"hrela_serviceid"}) is not None:
           results["score"] = soup.find(attrs={"id":"hrela_serviceid"}).text
       # parse_pic = soup.find(attrs={"data":"allImage"}).findAll("li")                                      #解析pictureUrl
        if soup.find(attrs={"data":"allImage"}):
             parse_pic = soup.find(attrs={"data":"allImage"}).findAll("li")
        else:
             parse_pic = []
        parse_list = []
        if len(parse_pic) > 0:
            for pict in parse_pic:
                if 'class' in pict.attrs:
                    results["picture_url"] = pict.findAll("img")[0].attrs["data-big"]
                    parse_list.append(results["picture_url"])
                else:
                    parse_list.append(pict.findAll("img")[0].attrs["data-big"])
            results["picture_list_url"] = parse_list
        parse_wifi = soup.find(attrs={"class":"dview_icon_list"}).findAll("li")                             #解析wifi、停车场、餐厅、健身房
        parse_wifi_dict = {"1":"无线WIFI","2":"停车场","3":"接机服务","4":"餐厅","5":"商务中心","6":"会议服务","7":"游泳池","8":"健身房","9":"宽带"}
        for data in parse_wifi:
            if 'class' not in data.attrs:
                wifi =  data.find("i").attrs["class"][0]
                wifi_ = wifi[len(wifi)-1]
                if wifi_ is 1:
                    results["has_wifi"]='1'
                elif  wifi_ is 2:
                    results["has_parking_lot"]='1'
                elif wifi_ is 4:
                    results["has_restaurant"]='1'
                elif wifi_ is 8:
                    results["has_gym"]='1'
        parsename = soup.find(attrs={"class":"link555 t12"}).findAll("a")                                   #解析城市中文名称
        city_name_cn = parsename[2].text
        results["city_name_cn"] = city_name_cn.replace("酒店","").replace("\t","").replace(" ","").replace("\n","")
        hotel_comment_msg = soup.find(attrs={"class":"hrela_comt bbddd hrela_comt1"})                   #解析好评率、总评论数等
       # print len(good_comment_rate)
        if  hotel_comment_msg.find(attrs={"class":"percentBox mr5 cur"}) is not None:
            good_comment_rate = soup.find(attrs={"id":"txt1"}).attrs["data-rate"]
            results["good_comment_rate"] = good_comment_rate
        if  hotel_comment_msg.find(attrs={"class":"hrela_comt_total"}) is not None:
            parsecomment_count =  soup.find(attrs={"class":"hrela_comt_total"}).find("a")
            if parsecomment_count is not None:
                results["total_comment_count"] =u'总评论'+ parsecomment_count.text

        #解析好評，中評，差評
        comment_msg1 = soup.find(attrs={"class": "dcomt_nav_list left"})
        if comment_msg1:
            comment_msg = comment_msg1.find_all("li")
            gassess = comment_msg[1].text
            bassess = comment_msg[2].text
            results['gassess'] = gassess
            results['bassess'] = bassess

        if soup.find(attrs={"calss":"dview_info_item dview_info_card"}) is not None:                   #解析是否支持信用卡
            results["is_credit_card_supportted"]='1'
        doc = soup.find(attrs={"class":"t24 yahei"}).find('b')                                             #解析level、type
        if doc!=None:
            classString = doc["class"]
            classStringList = classString#.split(' ')
           # print 'starOrLevel:',classStringList[1][len(classStringList[1])-1]
            starLevel = classStringList[1][len(classStringList[1])-1]
           # print 'level:',doc["title"]
           # item["level"]=doc["title"]
            results["type"]=doc["title"]
           # item["starLevel"]=starLevel
            results["level"]=starLevel
        else:
            starLevel = 2
            results["level"]=str(starLevel)
        hoteldoc = soup.find(attrs={"id":"hotelContent"}).findAll(attrs={"class":"dview_info_item"})  #解析酒店电话、开业时间等
        number = len(hoteldoc)
        for dlNumber in range(number):
             if hoteldoc[dlNumber].find('dt')!=None :
                if hoteldoc[dlNumber].find('dd')!=None:
                  dlStringKey = hoteldoc[dlNumber].find('dt').text
                  dlStringValue = hoteldoc[dlNumber].find('dd').text
                  if dlStringKey==u'酒店电话':
                   # print dlStringValue.split(u'艺龙')[0]
                    results["telephone"]=dlStringValue.split(u'艺龙')[0].replace("\n","").replace("\t","").replace(" ","")
                  if dlStringKey==u'开业时间':
                   # print dlStringValue
                    #item["basicInfo"]=dlStringValue  #重新解析，对字符串进行正则解析，解析出来对应的
                    dl_value = dlStringValue.replace("\n","").replace("\t","").replace(" ","").split("新近")
                    if len(dl_value) > 1:
                        results["opening_time"] = dl_value[0]
                        results["decorate_time"] = dl_value[1]
                    else:
                        results["opening_time"] = dl_value[0]
                        results["decorate_time"] = ''
                    # print results["opening_time"]
                    # print results["decorate_time"]
                  if dlStringKey==u'酒店设施':
                    results["facility"]=dlStringValue.replace("\n","").replace("\t","").replace(" ","")
                  if dlStringKey==u'酒店服务':
                     results["hotel_service"]=dlStringValue.replace("\n","").replace("\t","").replace(" ","")
                  if dlStringKey==u'酒店简介':
                    results["desc"]=dlStringValue.replace("\n","").replace("\t","").replace(" ","")
        #收藏数量
        favourite_count_url = "http://hotel.elong.com/ajax/detail/favhotle/favcount/?callback=jQuery1111014820067783092095_1467603429906&hotelId="+response.meta["hotelid"]
        yield Request(favourite_count_url, meta={'item':item,"results":results,'cityId':response.meta['cityId']}, callback=self.parse_favourite_count,dont_filter=True)

    def parse_favourite_count(self,response):  #解析收藏数量
        doc = response.body
        results = response.meta['results']
        lenth = len(doc)
        value =json.loads(doc[43:lenth-1])
        if len(value)>0:
              valuestr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":value}
              self.elong_pc_hotel_info_logger.info(valuestr)
        # print value
        results["favourite_count"] = str(value["value"])
        #周边景点
        surrAttraUrl = 'http://hotel.elong.com/ajax/detail/getpositionjva/?request.hotelId='+results["hotel_id"]+'&request.q='+u'景点'+'&request.lng='+results["longitude"]+'&request.lat='+results["latitude"]+'&request.page_num='+u'1'+'&request.filter='+u'1'
        yield Request(surrAttraUrl, meta={'item':response.meta['item'],"results":results,'cityId':response.meta['cityId']}, callback=self.parse_sur_sce,dont_filter=True)

    def parse_sur_sce(self, response):
         results = response.meta['results']
         if len(response.body)>17:
           surSceResJson = json.loads(response.body)
           logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surSceResJson}
           self.elong_pc_hotel_info_logger.info(logstr)
           # print len(surSceResJson)
           surSceValue = surSceResJson['value']
           trafficItems = surSceValue['trafficItems']
           number = len(trafficItems)
           # print number
           surrAttra = ""
           for i in range(number):
             name = trafficItems[i]["name"]
             distance = trafficItems[i]["distance"]
             surrAttra = surrAttra+str(i+1)+"##"+str(name)+"##"+u"直线距离"+str(distance)+"m;"
         #  item["surAttra"]=surrAttra
           results["surround_scenic"]=surrAttra.replace("\t","").replace(" ","").replace("\n","")
         surTraUrl = 'http://hotel.elong.com/ajax/detail/gettrafficjva/?cityId='+response.meta['cityId']+'&request.hotelId='+results["hotel_id"]+'&request.q='+u'长途汽车站'+'&request.lng='+results["longitude"]+'&request.lat='+results["latitude"]+'&request.page_num='+u'0'
         yield Request(surTraUrl, meta={'item':response.meta['item'],"results":results,'cityId':response.meta['cityId']}, callback=self.parse_sur_traffic,dont_filter=True)
    def parse_sur_traffic(self,response):
       # if len(response.body)>30:
        results = response.meta['results']
        try:
           surTraJson = json.loads(response.body)
           logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surTraJson}
           self.elong_pc_hotel_info_logger.info(logstr)
           surTraValue = surTraJson['value']
           surTraffic = []
           surTraValuelen = len(surTraValue)
           for j in range(surTraValuelen):
               type = surTraValue[j]["type"]
               # print type
               typeStr=''
               for k in range(surTraValue[j]["total"]):
                  typeStr =typeStr+str(k+1)+"##"+surTraValue[j]["trafficDetail"][k]["stationName"]+"##"+u"直线距离"+str(surTraValue[j]["trafficDetail"][k]["distance"])+u"m;"
               dict={type:typeStr}
               surTraffic.append(dict)
           # print surTraffic
           for data in surTraffic:
              # print data.keys()[0]
              if data.keys()[0] == 'air':
                 results["surround_airport"] = data["air"].replace("\t","").replace(" ","").replace("\n","")
              elif data.keys()[0] == 'train':
                  results["surround_metro"] = data["train"].replace("\t","").replace(" ","").replace("\n","")
              elif data.keys()[0] == 'bus':
                   results["surround_bus_station"] = data["bus"].replace("\t","").replace(" ","").replace("\n","")
           #item["surTra"]=surTraffic
           surhotUrl = 'http://hotel.elong.com/ajax/detail/getsurroundinghotels/?cityId='+response.meta['cityId']+'&cityNameEn='+results["city_name_gb"]+'&lat='+results["latitude"]+'&lng='+results["longitude"]+'&hotelId='+str(results["hotel_id"])+'&starLevel='+str(results["level"])
           # print surhotUrl
           yield Request(surhotUrl, meta={'item':response.meta['item'],"results":results}, callback=self.parse_sur_hotel,dont_filter=True)
        except:
           yield Request(surhotUrl, meta={'item':response.meta['item'],"results":results}, callback=self.parse_sur_traffic,dont_filter=True)

    def parse_sur_hotel(self,response):
         results = response.meta['results']
         if len(response.body)>75:
              surhotelJson = json.loads(response.body)
              logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surhotelJson}
              self.elong_pc_hotel_info_logger.info(logstr)
              surTraValue = surhotelJson['value']
              #最新预订
              results['latest_book'] = surTraValue.get("externalinfo")

              surTraContent = surTraValue["content"]
              # print '---------------------surhotelcontent------------------------------'
              soup = BeautifulSoup(surTraContent,"lxml")
              surHotelDoc = soup.findAll('li')
              # print surHotelDoc
              surHotelDoclen = len(surHotelDoc)
              # print surHotelDoclen
              # print '----xxxx------'
              surHotelTextString = ''
              for surHotelNumber in range(surHotelDoclen):
                  surHotelTextString=surHotelTextString+str(surHotelNumber+1)+'##'+str(surHotelDoc[surHotelNumber].find(attrs={"class":"dsider_per_text"}).find('a').text)+'##'+str(surHotelDoc[surHotelNumber].find(attrs={"class":"dsider_per_text"}).find('p').text)+';'
              #item["surHotel"]=str(surHotelTextString)
              results["surround_hotel"]=surHotelTextString.decode('utf8').replace("\t","").replace(" ","").replace("\n","")
              # print results
              #log.msg("The hotel Info item:::"+str(item),loglevel=log.INFO)
             # self.elonglogger.info(item)
              sur_food_url = "http://hotel.elong.com/ajax/detail/getpositionjva/?request.hotelId="+str(results["hotel_id"])+"&request.q=美食&request.lng="+results["longitude"]+"&request.lat="+results["latitude"]+"&request.page_num=1"
              yield  Request(sur_food_url, meta={'item':response.meta['item'],"results":results},callback=self.parse_surround_food,dont_filter=True)
         else:
              log.msg('elong error')

    def parse_surround_food(self,response):
        results = response.meta['results']
        surround_food =  json.loads(response.body)
        logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surround_food}
        self.elong_pc_hotel_info_logger.info(logstr)
        sur_food_total = surround_food['value']['total']
        surround_food_list=''
        if sur_food_total > 0:
            trafficItems = surround_food['value']['trafficItems']
            i = 1
            for data in trafficItems:
                # print data
              #  print data["price"]
                if "price" in data.keys():
                    surround_food_list =surround_food_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均"+str(data["price"])+"元##"+data["address"]+";"
                else:
                   surround_food_list =surround_food_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均0元##"+data["address"]+";"
                i= i+1
            results["surround_food"]=surround_food_list.replace("\t","").replace(" ","").replace("\n","")
        else:
            results["surround_food"]=''
        # print results
        surround_entertainment_url = "http://hotel.elong.com/ajax/detail/getpositionjva/?request.hotelId="+str(results["hotel_id"])+"&request.q=娱乐&request.lng="+str(results["longitude"])+"&request.lat="+str(results["latitude"])+"&request.page_num=1"
        yield  Request(surround_entertainment_url,meta={'item':response.meta['item'],"results":results},callback=self.parse_surround_entertainment,dont_filter=True)
    def  parse_surround_entertainment(self,response):
        results = response.meta['results']
        surround_entertainment =  json.loads(response.body)
        logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surround_entertainment}
        self.elong_pc_hotel_info_logger.info(logstr)
        surround_entertainment_total = surround_entertainment['value']['total']
        surround_entertainment_list=''
        if surround_entertainment_total > 0:
            trafficItems = surround_entertainment['value']['trafficItems']
            i = 1
            for data in trafficItems:
                if "price" in data.keys():
                    surround_entertainment_list =surround_entertainment_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均"+str(data["price"])+"元##"+str(data["address"])+";"
                else:
                    surround_entertainment_list =surround_entertainment_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均0元##"+str(data["address"])+";"
                i= i+1
            # print type(surround_entertainment_list)
            results["surround_entertainment"]=surround_entertainment_list.replace("\t","").replace(" ","").replace("\n","")
        else:
            results["surround_entertainment"]=''
        # print results
        surround_shopping_url = "http://hotel.elong.com/ajax/detail/getpositionjva/?request.hotelId="+str(results["hotel_id"])+"&request.q=购物&request.lng="+str(results["longitude"])+"&request.lat="+str(results["latitude"])+"&request.page_num=1"
        yield  Request(surround_shopping_url,meta={'item':response.meta['item'],"results":results},callback=self.parse_surround_shopping,dont_filter=True)
    def parse_surround_shopping(self,response):
        results = response.meta['results']
        surround_shopping =  json.loads(response.body)
        logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surround_shopping}
        self.elong_pc_hotel_info_logger.info(logstr)
        if 'value' in surround_shopping.keys():
            surround_shopping_total = surround_shopping['value']['total']
        else:
            surround_shopping_total=0
        surround_shopping_list=''
        if surround_shopping_total > 0:
            trafficItems = surround_shopping['value']['trafficItems']
            i = 1
            for data in trafficItems:
                if "price" in data.keys():
                    surround_shopping_list =surround_shopping_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均"+str(data["price"])+"元##"+str(data["address"])+";"
                else:
                    surround_shopping_list =surround_shopping_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均0元##"+str(data["address"])+";"
                i= i+1
            results["surround_shopping"]=surround_shopping_list.replace("\t","").replace(" ","").replace("\n","")
        else:
            results["surround_shopping"]=''
        # print results
        surround_rent_car_url = "http://hotel.elong.com/ajax/detail/getpositionjva/?request.hotelId="+results["hotel_id"]+"&request.q=租车&request.lng="+results["longitude"]+"&request.lat="+results["latitude"]+"&request.page_num=1"
        yield  Request(surround_rent_car_url,meta={'item':response.meta['item'],"results":results},callback=self.parse_surround_rent_car,dont_filter=True)
    def parse_surround_rent_car(self,response):
        results = response.meta['results']
        item = response.meta['item']
        surround_rent_car =  json.loads(response.body)
        logstr = {"id":results["hotel_id"],"timestamp":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"data":surround_rent_car}
        self.elong_pc_hotel_info_logger.info(logstr)
        surround_rent_car_total = surround_rent_car['value']['total']
        surround_rent_car_list=''
        if surround_rent_car_total > 0:
            trafficItems = surround_rent_car['value']['trafficItems']
            i = 1
            for data in trafficItems:
                if "price" in data.keys():
                   surround_rent_car_list =surround_rent_car_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均"+str(data["price"])+"元##"+str(data["address"])+";"
                else:
                   surround_rent_car_list =surround_rent_car_list+str(i)+"##"+data["name"]+"##直线约"+str(data["distance"])+"米##人均0元##"+str(data["address"])+";"
                i= i+1
            results["surround_rent_car"]=surround_rent_car_list.replace("\t","").replace(" ","").replace("\n","")
        else:
            results["surround_rent_car"]=''
        t = time.time()
        url = "http://hotel.elong.com/ajax/detail/gethotelreviewcommenttypesandroomtypes?hotelId=" + str(results["hotel_id"]) #+ '&_='+str(t)
        # url = "http://hotel.elong.com/ajax/detail/gethotelreviewcommenttypesandroomtypes?hotelId=42001245" + '&_='+str(t)

        yield  Request(url,meta={'item':response.meta['item'],"results":results},callback=self.parse_assess,dont_filter=True)

    #解析评价
    def parse_assess(self,response):
        body = json.loads(response.body)
        results = response.meta['results']
        item = response.meta['item']
        taCommentSummary = body.get("value").get("taCommentSummary")
        if taCommentSummary:
            results["trip_advisor_assess"] = taCommentSummary.get("totalComment")
        #印象评论数
        commentTagView = body.get("value").get("commentTagView")
        string1 = ""
        for assess_comment in commentTagView:
            mainName = str(assess_comment.get("MainName"))
            CommentCount = str(assess_comment.get("CommentCount"))
            string1 = string1 + " " + mainName+":" + CommentCount
            # 评论id:MainId
        #房型评论
        roomTypes = body.get("value").get("roomTypes")
        string2 = ""
        for room_assess in roomTypes:
            RoomtypeName = str(room_assess.get("RoomtypeName"))
            BadCount = str(room_assess.get("BadCount"))
            CommentCount = str(room_assess.get("CommentCount"))
            GoodCount = str(room_assess.get("GoodCount"))
            GoodRate = str(room_assess.get("GoodRate"))
            string2 = string2 + "\n" +RoomtypeName +":"+ ",BadCount = "+BadCount +\
                      ",CommentCount="+CommentCount +",GoodCount=" +GoodCount +",GoodRate=" +GoodRate

        results['room_assess'] = str(string2)
        results['assess_comment'] = str(string1)
        #评论总数
        str2= str(results.get("total_comment_count"))
        mod = re.compile(r'\d+')
        total_comment = mod.findall(str2)
        if total_comment is not None:
            if(int(total_comment[0])):
                ycount = int(total_comment[0])
                if (ycount) > 0 :
                    if ycount%20 == 0:
                        pageNum = ycount/20
                    else :
                        pageNum = ycount/20 + 1

                hotelid = str(results["hotel_id"])
                currorPage = 1
                # 发送具体评论信息
                url = 'http://hotel.elong.com/ajax/detail/gethotelreviews?hotelId='+hotelid+\
                  '&recommendedType=0&pageIndex='+str(currorPage) +'&roomTypeId=0&mainTagId=0&subTagId=0'
                yield Request(url=url,meta={'item':response.meta['item'],"results":results,"list":[],"currorPage":currorPage,"pageNum":pageNum},callback=self.parse_comment_content,dont_filter=True )

    # 爬具体评论信息
    def parse_comment_content(self,response):
        currorPage = response.meta.get("currorPage")
        pageNum = response.meta.get("pageNum")

        if (currorPage < pageNum ):
            body = json.loads(response.body)
            results = response.meta['results']
            item = response.meta['item']
            contents = body.get("contents")
            #存放评论dict
            list  = response.meta.get("list")
            #存放酒店的一条评论
            comment_content = {
                "commentid" : "",
                "name" : "",
                "content":""
            }
            for content in contents :
                com = comment_content.copy()
                com['commentid'] = content.get("commentId")
                com['content'] = content.get("content")
                com['name'] = content.get("commentUser").get("nickName")
                list.append(com)

            currorPage += 1
            url = 'http://hotel.elong.com/ajax/detail/gethotelreviews?hotelId=' + str(results["hotel_id"]) + \
                  '&recommendedType=0&pageIndex=' + str(currorPage) + '&roomTypeId=0&mainTagId=0&subTagId=0'
            yield Request(url=url,
                          meta={'item': response.meta['item'], "results": results, "list": list, "currorPage": currorPage,
                                "pageNum": pageNum}, callback=self.parse_comment_content, dont_filter=True)
        else:
            body = json.loads(response.body)
            results = response.meta['results']
            item = response.meta['item']
            contents = body.get("contents")
            # 存放评论dict
            list = response.meta.get("list")
            # 存放酒店的一条评论
            comment_content = {
                "commentid": "",
                "name": "",
                "content": ""
            }
            for content in contents:
                com = comment_content.copy()
                com['commentid'] = content.get("commentId")
                com['content'] = content.get("content")
                com['name'] = content.get("commentUser").get("nickName")
                list.append(com)
            results['content_assess'] = list
            item['results'] = results
            print item