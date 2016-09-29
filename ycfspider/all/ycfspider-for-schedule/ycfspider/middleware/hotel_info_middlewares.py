#coding=utf-8
import random
import time

from scrapy.utils.project import get_project_settings
from scrapy.http import Response
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.qunar_hota_login import QunarHotaLogin
from ycfspider.utils.qunar_zcf_login import QunarZcfLogin
from ycfspider.utils.ctrip_ebook_login import CtripEbookLogin


class HotelInfoMiddleware(object):
    handle_httpstatus_list = [302,403, 400, 401, 404, 405,409,500,501, 502,504]
    settings = get_project_settings()
    ctrip_pc_hotel_ycf_hoteliderror_logger = Logger(settings.get('LOG_PATH')+'/' + 'CtripPcHotelInfoSpider' + '/hoteliderror')
    qunar_hota_login = QunarHotaLogin()
    qunar_zcf_login = QunarZcfLogin()
    ctrip_ebook_login = CtripEbookLogin()
    user_agent_list = [\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"\
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",\
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",\
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",\
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
       ]
    cooikes = "CookieGuid=d18b30ce-d336-435d-b21c-3fc7b47ffa68; SessionGuid=cd346cf9-b75f-4b13-9449-023317f8881e; Esid=be7d62d2-c275-4cb6-87a3-f16f6ff63995; newjava2=1647fc5666785cbb2dc971b0ca7ce6cb; s_eVar44=ppzqbaidu; H5Channel=norefer-seo%2CSEO; H5CookieId=750875f1-b77c-4200-9bd2-031049d747df; H5-UA=7-6.1-1-50.0.2661; newjava1=ca92f417d91adf8d6b2db53fa1d89855; s_fid=558DEE462E0D39E2-1A3FEFF6CDA959A9; H5SessionId=C9F6D5C2A0F9942D854762DA633C0D66; TLTHID=7F8FC3BD482BF89E8D41A088E0834E91; TLTSID=7F8FC3BD482BF89E8D41A088E0834E91; JSESSIONID=673893E8826B302E7BF8C74319057456; _pk_ref.2624.42f4=%5B%22%22%2C%22%22%2C1467967401%2C%22http%3A%2F%2Fwww.elong.com%2F%22%5D; CitySearchHistory=1408%23%E6%BC%B3%E5%B7%9E%23Zhangzhou%23%401701%23%E9%83%91%E5%B7%9E%E5%B8%82%23Zhengzhou%23%402804%23%E5%BC%A0%E6%8E%96%23Zhangye%23%400101%23%E5%8C%97%E4%BA%AC%23Beijing%23; com.eLong.CommonService.OrderFromCookieInfo=Status=1&Orderfromtype=5&Isusefparam=0&Pkid=1003&Parentid=1500&Coefficient=0.0&Makecomefrom=1&Cookiesdays=30&Savecookies=0&Priority=9000; ShHotel=CityID=0101&CityNameCN=%E5%8C%97%E4%BA%AC%E5%B8%82&CityName=%E5%8C%97%E4%BA%AC%E5%B8%82&OutDate=2016-07-10&CityNameEN=Beijing&InDate=2016-07-09; s_cc=true; s_visit=1; s_sq=%5B%5BB%5D%5D; _pk_id.2624.42f4=e25f198d718d6362.1467194386.18.1467968750.1467967401.; _pk_ses.2624.42f4=*; SHBrowseHotel=cn=40101627%2C%2C%2C%2C%2C%2C%3B42705015%2C%2C%2C%2C%2C%2C%3B90214369%2C%2C%2C%2C%2C%2C%3B90642205%2C%2C%2C%2C%2C%2C%3B91348508%2C%2C%2C%2C%2C%2C%3B&"
    header_elong = {
        "Host":"hotel.elong.com",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate",
        "Accept-Language":"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection":"keep-alive",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0",
        "cookie":cooikes
    }
    hotel_ali = {
        "Host":"hotel.alitrip.com",
        "method":"GET",
        "Cache-Control":"max-age=0",
        "Connection":"keep-alive",
        "version":"HTTP/1.1",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-encoding":"gzip, deflate, sdch, br",
        "Accept-language":"zh-CN,zh;q=0.8",
        "Upgrade-Insecure-Requests":1,
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36"
    }
    cookie2='cna=spbvDzaDJmQCAXkh0qJVzjpM; checkInDate=2016-07-13; checkOutDate=2016-07-14; chanelStat="NQ=="; chanelStatExpire="2016-07-14 11:26:02"; _tb_token_=9FI5ZsgVIpv2; uss=VW7rVhqEcvZZbjbpDb%2FtTIv3RYCUjgBCZ10EUcb%2FgHbv7euj543u1olFNg%3D%3D; tracknick=%5Cu5F6A%5Cu6218%5Cu795E%5Cu8BDD; skt=f0ae6097c655f392; _l_g_=Ug%3D%3D; cookie2=1cc3ac03634739c3ec8b5db8ed379445; cookie1=UNIFF9dUMRTvoCKD50%2BhUIgc6Npl%2FSQL6axQ6PPsoZs%3D; unb=919973019; t=289cfe7d50c82990264996b5f2d60413; _nk_=%5Cu5F6A%5Cu6218%5Cu795E%5Cu8BDD; cookie17=WvECSvr0YbCs; uc1=cookie15=V32FPkk%2Fw0dUvg%3D%3D; CNZZDATA1253581663=1116174757-1468205972-https%253A%252F%252Fwww.alitrip.com%252F%7C1468370163; VISITED_HOTEL_TOKEN=5f917c72-ec19-4bba-82c2-e1819e4c9c86; JSESSIONID=E797CA99888B67EEBC8174048BB2B31B; l=AgwM02osTVB7oU-/aWAfop9mXGA8Q7Dv; isg=AkxME0vgdxo3nmPgR5TyfffpHarPODAsSbV7LqYPXveaMe87z5TfvyzHp47z'

    def process_request(self, request, spider):
        if spider.name in ["ElongPcHotelInfoSpider","CripPcHotelInfoSpider","QunarPcHotelInfoSpider","QunarPcScenicInfoSpider"]:
            if 'elong' in request.url:
                request.meta['header'] = self.header_elong
            # request.meta['proxy'] = "http://%s" % self.proxy_ip  #代理不稳定导致重试retrying
            ua = random.choice(self.user_agent_list)
            if ua:
                request.headers.setdefault('User-Agent', ua)

    def process_response(self, request, response, spider):  #retirecting 302
        #lvmama 500
        #处理ctrip酒店id错误导致的跳转
        # if response.status == 302:
        #         self.ctrip_pc_hotel_ycf_hoteliderror_logger.error(request.meta["item"])
        # if spider.name == 'QunarOtaPcHotaHotelInfoSpider':
        #     if response.url == 'http://hota.qunar.com/price/oapi/product/queryProducts':
        #         print response

        if spider.name in ["ElongPcHotelInfoSpider","CripPcHotelInfoSpider","QunarPcHotelInfoSpider","QunarPcScenicInfoSpider","MeituanAppScenicInfoSpider","CtripPcScenicInfoSpider"]:
            if 'ticket.lvmama.com' in response.url:
                if response.status in [404]:
                    return Response(response.url,status=200,body='')
                elif  response.status in self.handle_httpstatus_list:
                    return request
                else:
                    return response
            elif response.status in self.handle_httpstatus_list:
                  return request
            elif 'touch.qunar.com/api/hotel' in response.url:
                if response.body == '[]' or '<html>' in response.body:
                    return request
                return response
            # else:
            #     #继续处理
            #     return response
            if spider.name == 'CtripPcScenicInfoSpider':
                if  '您访问的太快了，休息一下吧。或者输入验证码继续访问'  in response.body:
                        i = random.randint(0,3)
                        time.sleep(i)
                        return request
                #偶尔的bug,访问次数过多产生多次跳转..
                if request.url == 'http://piao.ctrip.com':
                    return request.replace(url = request.meta['redirect_urls'](0))
        return response

