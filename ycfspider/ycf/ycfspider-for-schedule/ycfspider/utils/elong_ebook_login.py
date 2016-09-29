#coding=utf-8
import json
from StringIO import StringIO

import requests
import redis
#from PIL import Image

# from elong_captchas_break import ElongCaptcha
from scrapy.conf import settings
from ycfspider.utils.model_logger import Logger


# elongCaptcha = ElongCaptcha()
redis_key =  'login:elong_ota_login'
client = redis.Redis(host=settings.get('REDIS_HOST'),port=settings.get('REDIS_PORT'),db=0)
elong_ebook = Logger(settings.get('LOG_PATH') + '/ElongOtaPcEbookingHotelInfoSpider/usr_pwd_error/')
# logger = Logger('E:/qunar_elong_user_pwd_error/')
def login_page(username, password):
    while True:
        try:
            r = requests.session()
            headers = {
                'Accept': 'application/json, text/javascript, */*',
                'Accept-Language': 'zh-CN,zh;q=0.8',
                'Accept-Encoding': 'gzip, deflate, sdch',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
                'X-Requested-With': ' XMLHttpRequest',
                'Host':'ebooking.elong.com',
                'Content-Type':'application/json; charset=UTF-8',
                'Referer':'http://ebooking.elong.com/ebkauth/login'
            }
            url = 'http://ebooking.elong.com/ebkauth/login'
            r.get(url, headers=headers, timeout=3)
            url_check_username = 'http://ebooking.elong.com/ebkauth/ajaxNeedVerification'
            formdata = {"username": username, "password": "", "vcode": ""}
            headers['Referer'] = 'http://ebooking.elong.com/ebkauth/login'
            j = r.post(url=url_check_username, data=json.dumps(formdata), headers=headers,timeout=3).json()
            if j['data']:
                code_url = 'http://ebooking.elong.com/ebkauth/verifycode/code?type=1&7'
                response = r.get(code_url, timeout=3)
            #    img = Image.open(StringIO(response.content))
                # img.save('E:/test.png', 'PNG')
                #text = elongCaptcha.recognize(img)
                #通过接口发起请求
                files={'imagefile': ('test.png',StringIO(response.content), 'image/png')}
                text =  requests.post(url=settings.get('YZM_ADDRESS')+'/elong_capctha/', files=files).content
                vcode = ''
                if text:
                    vcode = eval(text).get('code')
                login_url = 'http://ebooking.elong.com/ebkauth/loginPost'
                post_data = {"username": username, "password": password, "vcode": vcode}
                login_result = r.post(login_url, headers=headers, data=json.dumps(post_data)).json()
                if login_result.get('errorMsg') == 'UsernameOrPasswrodError':
                    #{"code":"-400","data":false,"errorMsg":"UsernameOrPasswrodError","serverIp":"10.39.16.66","success":false}
                    elong_ebook.info('用户名或者密码错误：elong_ebooking_'+username+'_'+ password)
                    break
                if int(login_result['code']) == 0:
                   return r.cookies.get_dict()
                else:
                    print 'code error! change IP'
        except Exception,e:
            print e

def hotel_list():
    cookie = {'NSC_fcl_fc_ofx_80': 'ffffffffaf1d06da45525d5f4f58455e445a4a4229a0', 'EbkSessionId': '41110e26568b40589eba7f26ed4cfa0a', 'route': '589593221d5b43ac30a0f430993ba706', 'JSESSIONID': '0DAD7275A2493FB7D7720EF191FBE8E8'}
    headers = {
        'Accept': 'application/json, text/javascript, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With': ' XMLHttpRequest',
        'Host':'ebooking.elong.com',
        'Content-Type':'application/json; charset=UTF-8',
        'Referer':'http://ebooking.elong.com/ebkcommon/dashboard',
    }
    url = 'http://ebooking.elong.com/ebkcommon/currentHotel/selectHotelList'
    return requests.post(url, headers=headers, cookies=cookie, data='{}').json()

def hotel_room_type(hotel_id):
    cookie = {'NSC_fcl_fc_ofx_80': 'ffffffffaf1d06da45525d5f4f58455e445a4a4229a0',
              'EbkSessionId': '41110e26568b40589eba7f26ed4cfa0a', 'route': '589593221d5b43ac30a0f430993ba706',
              'JSESSIONID': '0DAD7275A2493FB7D7720EF191FBE8E8'}
    headers = {
        'Accept': 'application/json, text/javascript, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With': ' XMLHttpRequest',
        'Host': 'ebooking.elong.com',
        'Content-Type': 'application/json; charset=UTF-8',
        'Referer': 'http://ebooking.elong.com/ebkcommon/dashboard',
    }
    url = 'http://ebooking.elong.com/product/roomPrice/ajaxRoomRateList'
    formData = {'hotelId': hotel_id, 'roomType': "", 'startDate': "2016-08-23", 'endDate': "2016-09-05", 'productType': "0"}
    return requests.post(url, headers=headers, data=json.dumps(formData), cookies=cookie, timeout=3).json()

def get_user_cookie(user_key):
    hexists = client.hexists(redis_key,user_key)
    if hexists:
        return client.hget(redis_key,user_key)
    else:
        return 0

def set_user_cookie(user_key,cookie):
    #cookie是一个username:cookie的dict
    hexists = client.hexists(redis_key,user_key)
    if hexists :
        client.hdel(redis_key,user_key)
    if client.hset(redis_key,user_key,cookie):
        return 1
    else:
        return 0

def login(user_name,pwd,channel_number):
    cookie = login_page(user_name,pwd)
    if cookie:
        if set_user_cookie(user_name+'_'+channel_number,str(cookie)):
            return str(cookie)
        else:
                return None
    else:
        return None

if __name__=='__main__':
    print login_page('wujingli88', 'ycsyl123')
    # print hotel_list()
   # print hotel_room_type('90732568')