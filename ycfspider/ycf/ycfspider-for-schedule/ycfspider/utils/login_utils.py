#coding=utf-8
import json

import requests

from ycfspider.utils.qunar_zcf_ota_pwd_md5 import Password
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger
from ycfspider.utils.proxy_utils import ProxyUtil


class LoginUtil(object):
    qunar_login_header_1 = {
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Host':'cdycf.zcfgoagain.com',
        'Proxy-Connection':' keep-alive',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest'
    }
    qunar_login_header_2 = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/json;charset=UTF-8',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest',
        'Host':'cdycf.zcfgoagain.com'
    }
    qunar_login_header_3 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/json;charset=UTF-8',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest',
        'Host':'cdycf.zcfgoagain.com'
    }
    qunar_hota_login_header_1 = {
        'Accept':'*/*',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Host':'shanghu.qunar.com',
        'Proxy-Connection':' keep-alive',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest'
    }
    qunar_hota_login_header_2 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest',
        'Host':'hota.qunar.com'
    }
    ctrip_login_header_1 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch",
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': 1,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'Host': 'www.vipdlt.com',
}

    def __init__(self):
        # self.logger = Logger('E:/qunar_ctrip_user_pwd_error/')
        self.ctrip_data = Logger(settings.get('LOG_PATH') + '/CtripOtaPcEbookingHotelInfoSpider/usr_pwd_error/')
        self.qunar_zcf = Logger(settings.get('LOG_PATH') + '/QunarOtaPcZcfHotelInfoSpider/usr_pwd_error/')
        self.qunar_hota = Logger(settings.get('LOG_PATH') + '/QunarOtaPcHotaHotelInfoSpider/usr_pwd_error/')
        self.proxy_util = ProxyUtil()


    def qunar_zcf_login(self,user_name,user_pawd,login_url):
        #
        s = requests.Session()
        login_count = 3
        mycookie = ''
        while login_count:
            try:
                proxy = {'http':''}
                proxy['http'] =  self.proxy_util.find_one_proxy(2)#self.proxy_util.get_proxy()
                if  proxy['http']:
                    url_1 =login_url +  '/rbac/api/usr/getLoginToken?userName='+user_name+'&_time='
                    res_1 = s.get(url_1,headers=self.qunar_login_header_1,proxies=proxy,verify=False,allow_redirects=False,timeout=5)
                    body_1 = json.loads(res_1.content)
                    data_1=''
                    if body_1:
                        if 'data' in body_1.keys():
                            data_1 = body_1['data']
                    if data_1:
                        password_md5 = Password()
                        password = password_md5.parse_password_md5(user_pawd)
                        password = password_md5.parse_user_cookie_md5(password, user_name, data_1)
                        url_2 = login_url + '/rbac/api/usr/login'
                        formdata = {"userName": user_name, "password": password, "auto": 0, "redirectUrl": ""}
                        # params=None,data=None,headers=None,cookies=None,files=None,auth=None,timeout=None,allow_redirects=True,proxies=None,hooks=None,stream=None,verify=None,cert=None,json=None):
                        res_2 = s.post(url_2,data = json.dumps(formdata),proxies=proxy,headers=self.qunar_login_header_2,verify=False,allow_redirects=False,timeout=5)
                        #print res_2.content
                        if '用户名或密码错误' in str(res_2.content):
                        #{"ver":"1.0","ret":false,"errmsg":"用户名或密码错误","errcode":4}
                        #write_2_mongo
                            # print u'用户名或密码错误'
                            self.qunar_zcf.info('用户名或者密码错误：qunar_zcf_'+user_name+'_'+ user_pawd)
                            break
                        mycookie =  s.cookies.get_dict()
                        break
            except Exception, e:
                print e
                # print e
            finally:
                login_count -= 1
        if mycookie:
            # print u'登陆成功'
            return mycookie
        else:
            # print u'登录失败'
            return None
            # url_test = 'http://cdycf.zcfgoagain.com/confirm/api/list/queryOrderList?currentPage=1&' \
            #       'pageSize=10&showType=mylist&parentOrderNum=&customerName=&orderDateStart=2016-07-17' \
            #       '&orderDateEnd=2016-08-16&hotelName=&contactPhone=&fromDateStart=&fromDateEnd=&parentOrderStatus' \
            #       '=&payOrderStatus=&orderSource=&channel=&subOrderStatus=&orderPerson=&cityName=&subOrderNum=&third' \
            #       'PartyNum=&contactName=&supplierName=&distributorName=&toDateStart=&toDateEnd=&operator=&agentCode=&bizType='
            # # params=None,data=None,headers=None,cookies=None,files=None,auth=None,timeout=None,allow_redirects=True,proxies=None,hooks=None,stream=None,verify=None,cert=None,json=None):
            # res_3 = requests.get(url_test,params={'test':'test'},headers=self.qunar_login_header_3,verify=False,allow_redirects=False,cookies=mycookie)
            # print res_3.content
            # print res_3.txt
    def qunar_hota_login(self,user_name,user_pawd):
        #增添一些校验信息，如果登陆失败则将登陆失败则重试一次，如果失败两次则将其写入登陆失败日志
        s = requests.Session()
        login_count = 3
        mycookie = ''
        while login_count:
            try:
                proxy = {'http':''}
                path = ''
                # if login_count == 3:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=ctrip'
                # elif login_count == 2:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=elong'
                # elif login_count == 1:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=qunar'
                # if path:
                #     proxy['http'] ='http://'+ requests.get(path).content
                proxy['http'] =  self.proxy_util.find_one_proxy(1)#self.proxy_util.get_proxy()
                if  proxy['http']:
                    formdata_1 = {'username':user_name,'password':user_pawd,'captcha':'','remember':'false','ret':'http%3A%2F%2Fhota.qunar.com%2Fstats%2Fohtml%2Fannouncement%2FqueryAnnouncements',}
                    url_1 = 'https://shanghu.qunar.com/passport/doLogin'
                    res_1 = s.post(url_1,data=formdata_1,headers=self.qunar_hota_login_header_1,proxies=proxy,verify=False,allow_redirects=False)
                    if '用户名或密码错误' in str(res_1.content):
                        #write_2_mongo
                        #{"ver":1,"ret":false,"errcode":21022,"errmsg":"商户名或密码错误","errkey":null,"data":{"needCaptcha":false}}
                        # print u'商户名或密码错误'
                        self.qunar_hota.info('商户名或密码错误：qunar_hota_'+user_name+'_'+ user_pawd)
                        break
                    mycookie = s.cookies.get_dict()
                    break
            except Exception,e:
                print e
            finally:
                login_count -= 1
        if mycookie:
            # print u'登陆成功'
            return mycookie
        else:
            # print u'登录失败'
            return None
        # url_2 = 'http://hota.qunar.com/stats/ohtml/announcement/queryAnnouncements'
        # res_2 = s.get(url_2,headers=self.qunar_hota_login_header_2,verify=False,allow_redirects=False)
        # return res_2.cookies.get_dict()

    def ctrip_ota_login(self,user_name,user_pwd):
        s = requests.Session()
        login_count = 3
        mycookie = ''
        while login_count:
            try:
                proxy = {'http':''}
                # path = ''
                # if login_count == 3:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=ctrip'
                # elif login_count == 2:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=elong'
                # elif login_count == 1:
                #     path = settings['PROXY_ADDRESS']+'/getIp?type=qunar'
                # if path:
                #     proxy['http'] ='http://'+ requests.get(path).content
                proxy['http'] =  self.proxy_util.find_one_proxy(1)#self.proxy_util.get_proxy()
                if  proxy['http']:
                    url_0 = 'http://ebooking.ctrip.com/ebkassembly/login.aspx'
                    s.get(url_0,verify=False,allow_redirects=False,proxies=proxy,headers=self.ctrip_login_header_1,timeout=5)
                    url_1 =  'http://www.vipdlt.com/ebkassembly/login/LoginProcess.ashx'
                    formata_1 = {"Method":"AccLogin","UserName":user_name,"UserPwd":user_pwd}
                    res_1 = s.post(url_1,data=formata_1,verify=False,allow_redirects=False,proxies=proxy,headers=self.ctrip_login_header_1,timeout=5)
                    body_1 = json.loads(res_1.content)
                    if '用户名或密码错误' in str(body_1):
                        #write_2_mongo
                        #{"Rcode":0,"Msg":"用户名或密码错误","Data":{"LoginStatus":-500,"URL":null,"Randomkey":null,"SetCtripCookiesUrl":"http://ebooking.ctrip.com/ebkassembly/SetR.aspx","SetDltCookiesUrl":"http://www.vipdlt.com/ebkassembly/SetR.aspx","rToken":null,"rHuid":null}}
                        self.ctrip_data.info('用户名或者密码错误：ctrip_ebooking_'+user_name+'_'+ user_pwd)
                        # print u'用户名或密码错误'
                        break
                    if 'Data' in body_1.keys():
                        data = body_1["Data"]
                       # if 'URL' in data.keys():
                        url_2 = data.get("URL")
                        res_2 = s.get(url_2,verify=False,allow_redirects=False,headers=self.ctrip_login_header_1,timeout=5)
                        mycookie = s.cookies.get_dict()
                        break
                            #else:
                            #    self.ctrip_ota_login(user_name,user_pwd)
                               #  url_3 = 'http://www.vipdlt.com/MIP/Order/MIP/OrderList.aspx'
                               #  res_3 = requests.get(url_3,verify=False,allow_redirects=False,cookies=mycookie)
                               #  print res_3.content
            except Exception, e:
                print e
            finally:
                login_count -= 1
        if mycookie:
            # print u'登陆成功'
            return mycookie
        else:
            # print u'登录失败'
            return None


if __name__ == "__main__":
    pass
    # print r.push('task:1', 3)
    # l = LoginUtil()
    # #l.qunar_zcf_login('readgo','yaochufa123')
    # l.qunar_zcf_login('admin','ycf2016@','http://cblyw.zcfgoagain.com')
    # # l.qunar_hota_login('ha8et:admin', 'yaochufa2016')
