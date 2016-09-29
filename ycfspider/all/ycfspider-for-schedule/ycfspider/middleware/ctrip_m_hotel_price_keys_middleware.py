#encoding=utf-8

import json

from ycfspider.utils.ctrip_fc import CtripJs
__author__ = 'lizhipeng'

class CtripMHotelPriceKeysMiddleware(object):

     def __init__(self):
          self.js = CtripJs()
          # self.key = ''

     def process_request(self, request, spider):
          if spider.name is 'CtripMHotelPriceSpider':
               body_json = json.loads(request.body)
               proxy = request.meta['proxy']
               hotel_id = request.meta['hotel_id']
               guid = request.meta['guid']
               check_in_date = request.meta['check_in_date'].replace('-', '')
               cookie = request.cookies
               # if self.key == '':
               self.key = self.js.ctrip_m_key(str(hotel_id), guid, check_in_date, proxy, cookie)
               body_json['Key'] = self.key
               request._set_body(json.dumps(body_json))


