# -*- coding: UTF-8 -*-
import base64
import json
import math
import random
import time
import gc

import requests
import PyV8
from PyV8 import JSEngine

from scrapy import log


class CtripJs():

    def __init__(self):
        self.ctxt = None



    def get_fc(self,hotel_id,check_in_date):
        check_in_date = check_in_date.replace('-','')
        t = int(hotel_id) + int(check_in_date)
        s = base64.b64encode(str(t))
        k = list(s)
        s = ''
        for i in range(0, len(k)):
            s += k[i]
            if i == 0 or i == 2 or i == 5 or i == 9 or i == 14:
                s += k[len(k) - i - 1]
        return s

    def get_client_id(self,proxy):
        try:
            headers = {
                'Host': 'm.ctrip.com',
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36',
            }
            url = 'http://m.ctrip.com/restapi/soa2/10290/createclientid?systemcode=09&createtype=3&contentType=json'
            json = requests.get(url, proxies={'http': 'http://%s' % proxy}, headers=headers, timeout=3).json()
            return json['ClientID']
        except Exception,e:
            return None

    def nodejs(self, n, t):
         func = self.ctxt.eval('''
        (function(n, t) {
        var r, o, e = "1", i = void 0 == e[0], c = i ? [] : "";
        for (r = 0; r < n.length; r++){
            o = t.charAt(n.charAt(r).charCodeAt(0) - 21760).charAt(0),
            i ? c.push(o) : c += o;
            }
        return c = i ? c.join("") : c
         })''')
         return func(n, t)

    def ctrip_enevn(self,hotel_id,proxy):
        n = self.e(15)
        t = int(time.time() * 1000)
        url = 'http://hotels.ctrip.com/domestic/cas/oceanball?callback=%s&_=%s' % (n, t)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
                   'Referer': 'http://hotels.ctrip.com/hotel/%s.html?isFull=F' % hotel_id,
                   'Host': 'hotels.ctrip.com',
                   'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
                   'Accept-Encoding': 'gzip, deflate, sdch'}
        content = requests.get(url, headers=headers, proxies={'http': 'http://%s' % proxy}).content
        content = content.replace('eval', '')
        s = self.ctxt.eval(content)
        s = s.replace(';!function(){', '')
        index = s.find(n)
        s = s[0:index]
        s = self.ctxt.eval(s)
        return s

    def get_ctrip_price(self, hotel_id,startDate,depDate,proxy):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
            'Referer': 'http://hotels.ctrip.com/hotel/%s.html?isFull=F' % hotel_id,
            'Host': 'hotels.ctrip.com',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'Accept-Encoding': 'gzip, deflate, sdch'}
        t = int(time.time() * 1000)
        eleven = self.ctrip_enevn(hotel_id, proxy)
        callback = self.e(16)
        url = 'http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=' \
              '&MasterHotelID='+ hotel_id +'&hotel=' + hotel_id+ '&EDM=F&roomId=&IncludeRoom=&city=32&showspothotel=T&' \
              'supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=69&startDate='+startDate+'&' \
              'depDate='+depDate+'&RequestTravelMoney=F&hsids=&IsJustConfirm=\&contyped=0&priceInfo=-1&' \
              'equip=&filter=&productcode=&couponList=&abForHuaZhu=&TmFromList=F&eleven='+eleven+'&callback='+callback+'&_='+ str(t)
        req = requests.get(url, headers=headers, proxies={'http': 'http://%s' % proxy}).json()
        return self.nodejs(req['ComplexHtml'], req['ASYS'])

    def ctrip_m_key(self, hotel_id, cid, startDate, proxy, cookie):
        # self.ctxt = PyV8.JSContext()
        # self.ctxt.enter()
        with PyV8.JSContext() as ctxt:
            referer = 'http://m.ctrip.com/webapp/hotel/hoteldetail/%s.html?days=1&atime=%s&contrl=2&num=1&biz=1' % (hotel_id, startDate.replace('-', ''))
            headers = {
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36',
                'Referer': referer,
                'Host': 'm.ctrip.com',
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate',
                'Content-Type': 'application/json',
                # 'Cookie': cookie
            }
            url = 'http://m.ctrip.com/restapi/soa2/10934/hotel/customer/cas?_fxpcqlniredt=%s' % cid
            callback = self.randomString(17)
            post_data = {
                        "callback": callback,
                        "alliance": {
                            "ishybrid": 0
                        },
                        "head": {
                            "cid": cid,
                            "ctok": "",
                            "cver": "1.0",
                            "lang": "01",
                            "sid": "8888",
                            "syscode": "09",
                            "auth": None,
                            "extension": [{
                                "name": "pageid",
                                "value": "212094"
                            },
                            {
                                "name": "webp",
                                "value": 1
                            },
                            {
                                "name": "referrer",
                                "value": "http://m.ctrip.com/html5/"
                            },
                            {
                                "name": "protocal",
                                "value": "http"
                            }]
                        },
                        "contentType": "json"
                        }
            proxies = {'http':  proxy}
            req = requests.post(url, headers=headers, data=json.dumps(post_data), proxies=proxies, cookies=cookie, timeout=3).json()
            content = req['Script']
            content = content.replace('eval', '')
            s = ctxt.eval(content)
            s = s.replace(';!function(){', '')
            index = s.find(callback)
            s = s[0:index]
            s = s.replace('this.location.href', '"%s"' % referer)
            s = ctxt.eval(s)
            # self.ctxt.leave()
            # self.ctxt.lock.leave()
            log.msg('keys:' + s , level=log.INFO)
            # print s
            JSEngine.collect()
            gc.collect()
            JSEngine.collect()
            gc.collect()
            return s

    def e(self,e):
        t = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
             "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
             "s", "t", "u", "v", "w", "x", "y", "z"]
        o = "CAS"
        for i in range(0, e):
            a = int(math.ceil(51 * random.random()))
            o += t[a]
        return o

    def randomString(self,e):
        t = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
             "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
             "s", "t", "u", "v", "w", "x", "y", "z"]
        n = ""
        for r in range(0, e):
            i = int(math.ceil(51 * random.random()))
            n += t[i]
        return n


if __name__ == '__main__':

    while True:
        cookie = '_abtest_userid=6d2e6e82-87b2-49be-9bb5-6e6ecc8509a2; Session=SmartLinkCode=U111732&SmartLinkKeyWord=&SmartLinkQuary=&SmartLinkHost=&SmartLinkLanguage=zh; Union=AllianceID=4931&SID=111732&OUID=000401app-; adscityen=Guangzhou; __zpspc=9.1.1472034757.1472034757.1%234%7C%7C%7C%7C%7C%23; ASP.NET_SessionSvc=MTAuMTUuMTI4LjM0fDkwOTB8b3V5YW5nfGRlZmF1bHR8MTQ3MDMwMTM1NjA5OA; appFloatCnt=1; NSC_WT_Xbq_80=ffffffff090025c745525d5f4f58455e445a4a423660; _fpacid=09031015310338255807; GUID=09031015310338255807; ASP.NET_SessionId=eu1vv54aatyz3qnlukxh2qfq; _bfa=1.1472034755409.1v7yd5.1.1472034755409.1472034755409.1.12; _bfs=1.12; _jzqco=%7C%7C%7C%7C1472034757815%7C1.1693022637.1472034757729.1472036481879.1472037108570.1472036481879.1472037108570.0.0.0.6.6; _ga=GA1.2.1768391939.1472034758; _gat=1; _bfi=p1%3D212094%26p2%3D212094%26v1%3D12%26v2%3D11'
        c =CtripJs()
        c.ctrip_m_key('3418826','09031015310338255807', '2016-08-24', None,cookie)
        time.sleep(2)
        c = None
    # v_ip_thread = threading.Thread(target=c.ctrip_m_key, args=('3418826','09031119110332709387', '2016-08-24', None,))
    # v_ip_thread.start()
    # pool = multiprocessing.Pool(processes=4)
    # p = multiprocessing.Process(target=c.ctrip_m_key, args=('3418826','09031119110332709387', '2016-08-24', None,))
    # p.start()