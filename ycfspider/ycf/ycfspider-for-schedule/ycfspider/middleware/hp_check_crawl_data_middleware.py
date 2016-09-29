#encoding=utf-8
import json
import traceback
import requests

from scrapy.conf import settings

from ycfspider.middleware.proxy_middleware import ProxyMiddleware
from ycfspider.utils.model_logger import Logger

__author__ = 'lizhipeng'

class HpCheckCrawlDataMiddleWare(object):
    # 对爬虫返回数据进行检验，如果返回数据为非法的，重新抓取

    def __init__(self):
        # super(ProxyMiddleware, self).__init__()
        self.logger = ''

    def process_response(self,request, response, spider):
        body = response.body
        if self.logger == '':
            self.logger = Logger(settings['LOG_PATH']+'/'+spider.name+'/proxy_error/')
        try:
            # 如果解析报错则重新请求，并记录请求次数，超过3次，放弃。
            if spider.name == 'ElongPcHotelPriceSpider':
                jsonDic = json.loads(body)
                valueDic = jsonDic["value"]
                html = valueDic["content"]
                productsInfo = valueDic['hotelTipInfo']['productsInfo']
            elif spider.name == 'CtripMHotelPriceSpider':
                json_get_values = json.loads(body)
                rooms = json_get_values['rooms']
                rooms[0]['bid']
            elif spider.name == 'QunarPcHotelPriceSpider':
                body = response.body
                hotel_response_content = body.lstrip('(')
                hotel_response_content = hotel_response_content.rstrip(';')
                hotel_response_content = hotel_response_content.rstrip(')')
                room_infos_json = json.loads(hotel_response_content, encoding='utf-8')
                rooms_dict = room_infos_json['result']
                if not rooms_dict:
                    # 当result为空时，需要请求初始化价格信息接口
                    self.requests_qunar_init_api(request)
                    raise Exception('qunar should init api')
                for key in rooms_dict.keys():
                    room_id = int(rooms_dict[key][12])
            return response
        except Exception, e:
            # print traceback.format_exc()
            # self.logger.info('proxy:' + self.proxy)
            self.logger.error(traceback.format_exc())
            if request.meta['retry_count'] <= 2:
                # request.meta['proxy'] = self.get_proxy(spider.name)
                request.meta['retry_count'] += 1
                return request
            else:
                return response

    def requests_qunar_init_api(self, request):
        # 初始化去哪儿网价格信息接口请求
        headers = request.headers
        meta = request.meta
        proxy = {
            'http': meta['proxy']
        }
        hotel_id_split = meta['hotel_id'].split('_')
        city_id = ''
        id_num = hotel_id_split[-1]
        if len(hotel_id_split) > 2:
            for i in range(0, len(hotel_id_split)-1):
                city_id = city_id + hotel_id_split[i] + '_'
            city_id = city_id.rstrip('_')
        else:
            city_id = hotel_id_split[0]
        init_url = 'http://hotel.qunar.com/render/detailInit.jsp?requestor=DETAIL_S_D&cityurl=%s&fromDate=%s&toDate=%s&HotelSEQ=%s&filterid=29c20e99-f82c-43e2-b659-d26226188ca7'% \
                   (city_id, meta['check_in_date'], meta['check_out_date'], meta['hotel_id'])
        _session1 = requests.Session()
        _session1.headers.update(headers)
        try:
            _session1.get(init_url, proxies=proxy, timeout=5)
        except:
            print traceback.format_exc()
        request.meta['init_times'] = request.meta.get('init_times', 0) + 1