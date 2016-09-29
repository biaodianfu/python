#coding=utf-8
import urllib2
import random,time
from scrapy.http import Response
from ycfspider.utils.qunar_hota_login import QunarHotaLogin
from ycfspider.utils.qunar_zcf_login import QunarZcfLogin
from ycfspider.utils.ctrip_ebook_login import CtripEbookLogin
from ycfspider.utils import elong_ebook_login

class OtaMiddleware(object):
     def __init__(self):
         self.qunar_hota_login = QunarHotaLogin()
         self.qunar_zcf_login = QunarZcfLogin()
         self.ctrip_ebook_login = CtripEbookLogin()

     def process_request(self, request, spider):
        if spider.name in ['QunarOtaPcHotaHotelInfoSpider','CtripOtaPcEbookingHotelInfoSpider','QunarOtaPcZcfHotelInfoSpider','ElongOtaPcEbookingHotelInfoSpider']:
            i = random.randint(0,10)
            time.sleep(i)
            # print 'random sleep '+ str(i) + ' seconds'
     def process_response(self, request, response, spider):  #retirecting 302
        #zcf大量抓取的时候可能会封锁ip，导致大量的ip不能使用
        if spider.name in ['QunarOtaPcHotaHotelInfoSpider','CtripOtaPcEbookingHotelInfoSpider','QunarOtaPcZcfHotelInfoSpider','ElongOtaPcEbookingHotelInfoSpider']:
            #此处的response情况还需要慢慢的完善
            if spider.name == 'QunarOtaPcHotaHotelInfoSpider':
                if '用户名或密码错误' in response.body  or  '需要登录'  in response.body :
                    cookie  =  self.qunar_hota_login.login(request.meta['item']['user_name'],request.meta['item']['password'],request.meta['item']['channel_number'])
                    if cookie:
                        #重构request,修正cookies参数
                        request.cookies = eval(cookie)
                        return request
                    else:
                        #需要对没有登陆成功的response进行处理
                        return Response(response.url,status=200,body={})
                else:
                    return response
            if spider.name == 'QunarOtaPcZcfHotelInfoSpider':
                if '用户名或密码错误' in response.body  or  '需要登录'  in response.body :
                    cookie  =  self.qunar_zcf_login.login(request.meta['ritem']['user_name'],request.meta['ritem']['password'],request.meta['ritem']['login_url'],request.meta['ritem']['channel_number'])#[item['user_name']]
                    if cookie:
                        #重构request,修正cookies参数
                        request.cookies = eval(cookie)
                        return request
                    else:
                         return Response(response.url,status=200,body='{}')
                else:
                    return response

            if spider.name == 'CtripOtaPcEbookingHotelInfoSpider':
                if '验证访问' in response.body  or  '请输入验证码'  in response.body or  '需要登录'  in response.body :
                    cookie  =  self.ctrip_ebook_login.login(request.meta['ota_user_pwd']['user_name'],request.meta['ota_user_pwd']['password'],request.meta['ota_user_pwd']['channel_number'])#[item['user_name']]
                    if cookie:
                        #重构request,修正cookies参数
                        request.cookies = eval(cookie)
                        return request
                    else:
                         return Response(response.url,status=200,body={})
                else:
                    return  response
            if spider.name == 'ElongOtaPcEbookingHotelInfoSpider':
                if response.status == 222 :
                    #重新登录
                    #cookie  =  elong_login.login(response.meta['username'],response.meta['password'],response.meta['channel_number'])
                    cookie  =  elong_ebook_login.login(request._meta['user_name'], request._meta['password'], request._meta['channel_number'])
                    if cookie:
                        #重构request,修正cookies参数
                        request.cookies = eval(cookie)
                        #response.meta['cookie'] = eval(cookie)
                        return request
                    else:
                        return Response(response.url,status=200,body={})
                else:
                    return response
        return response


if __name__ == "__main__":
    l = OtaMiddleware()
    l.process_request()
