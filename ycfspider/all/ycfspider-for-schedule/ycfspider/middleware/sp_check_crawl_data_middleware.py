# encoding=utf-8
import json
import traceback

from scrapy.conf import settings
from scrapy.http import Response

from ycfspider.middleware.proxy_middleware import ProxyMiddleware
from ycfspider.utils.model_logger import Logger

__author__ = 'lizhipeng'


class SpCheckCrawlDataMiddleWare(object):
    # 对爬虫返回数据进行检验，如果返回数据为非法的，重新抓取

    def __init__(self):
        # super(ProxyMiddleware, self).__init__()
        self.logger = ''
        self.max_retry_times = settings.getint('RETRY_TIMES')

    def process_response(self, request, response, spider):
        body = response.body
        if self.logger == '':
            self.logger = Logger(settings['LOG_PATH'] + '/' + spider.name + '/proxy_error/')
        try:
            if spider.name == 'MeituanAppScenicPriceSpider':
                ticket_json = json.loads(body)
            if spider.name == 'CtripPcScenicPriceSpider':
                if 'Thingstodo-Booking-BookingWebSite' in request.url:
                    ticket_json = json.loads(body)
            if spider.name == 'QunarPcScenicPriceSpider':
                # 景区价格
                if 'piao.qunar.com' in response.url:
                    if u'未获取到有效的日历报价信息' in response.body:
                        url = 'http://piao.qunar.com/order/getTypeProductValidDate.json?method=queryProductDate&productId=%s' % \
                              request.meta['productId']
                        request._set_url(url)
                        raise Exception(u'no calender price')
                    else:
                        data_j = json.loads(response.body)
                        if 'ticket' in response.url:
                            data = data_j['data']['groups']
                        else:
                            data = data_j['data']
            if spider.name == 'LvmamaPcScenicPriceSpider':
                if response.status in [404]:
                    return Response(response.url, status=200, body='')
                if 'ticket.lvmama.com' not in response.url:
                    data = json.loads(response.body)
            return response
        except:
            if request.meta.get('redirect_times'):
                request._set_url(request.meta.get('redirect_urls')[0])
            log_data = {}
            log_data['url'] = request.url
            if 'getTickets.json' in request.url:
                log_data['url'] += '  ' + str(request.meta.get('id'))
            log_data['error_info'] = traceback.format_exc()

            if spider.name == 'QunarPcScenicPriceSpider':
                retries = request.meta.get('retry_times', 0)
                if retries < self.max_retry_times:
                    request.meta['retry_times'] = retries+1
                else:
                    if 'getTickets.json'not in request.url:
                        log_data['other_info'] = 'product_id:' + request.meta.get('productId') + '  ' + 'scenic_id:' + request.meta.get('sightId')
                    return response
            self.logger.error(json.dumps(log_data))
            # self.logger.error('id:'+request.meta.get('ticket_item').get('ticket_id')+'\n'+'url:'+request.url + '\n'+'proxy:'+request.meta['proxy']+'\n')
            return request
