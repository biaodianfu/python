#encoding=utf-8
import requests
import random
import traceback

from pymongo import MongoClient
from pymongo.collection import Collection
from scrapy.http import Response
from scrapy.conf import settings

__author__ = 'lizhipeng'


class QunarPcHotelPriceRequestsMiddleware(object):

    def __init__(self):
        client = MongoClient(settings.get('MONGODB_HOST'), int(settings.get('MONGODB_PORT')))
        db = client[settings.get('MONGODB_DATABASE')]
        self.collection = Collection(db, 'qunar_cookie')

    def process_request(self, request, spider):

        if spider.name is 'QunarPcHotelPriceSpider':
            response = None
            # ip = 'http://117.136.234.8:80'
            proxy = {
                    'http': request.meta['proxy']
             }
            headers = request.headers
            headers['Cookie'] = self.get_cookie()
            _session = requests.Session()
            _session.headers.update(headers)
            content = ''
            try:
                search_response = _session.get(request.url, proxies=proxy, timeout=5)
                content = search_response.content
            except Exception, e:
                response = Response(url=request.url, headers=request.headers, body='', request=request, status=404)
                return response
            response = Response(url=request.url, headers= request.headers, body=content, request=request)
            return response

    def process_exception(self, request, exception, spider):
        if spider.name is 'QunarPcHotelPriceSpider':
            if request.meta.get('retry_times', 0) == settings.getint('RETRY_TIMES'):
                self.logger.info('proxy retry '+str(request.meta.get('retry_times', 0))+' times fail, spider:'+
                                 spider.name + ' url:' + request.url)
            response = Response(url=request.url, headers=request.headers, body='', request=request, status=200)
            return response

    def get_cookie(self):
        cookie_list = list(self.collection.find({}))
        # cookie_list = [
        #     {'cookie':'QN70=0d8cc5e53155e1f33aec; __ag_cm_=1468374675051; pgv_si=s3860911104; __utmt=1; _jzqx=1.1446808055.1468374703.6.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/huizhou_guangdong/dt-2030/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/guangzhou/dt-16582/; _jzqckmp=1; QN25=771d9af1-b841-4762-9d0c-4c9c0d5c78d1-9f992f90; QN271=d5ede5fb-0a3b-4b8c-83f0-9fe0e06c198d; QN42=ogqf2375; _q=U.wskaiwx8424; _t=24582352; csrfToken=vgxqQleIfMTUOkjwmj9tNpmPkjc3dsdP; _s=s_IV6V75ESSZ4IRVVVWMVB732T7E; _v=0K0GmtwD0XPNN_Sy8FUb8zd7Zd8-nPPlfEqmabUCWViVP6oJflEXu3GXPESRPq9R9RvvCjHYjCpbI3MwPnWuXCAtqAj0whlOgO7u4Pkqjsx_NDPZX5rCiqgVUlSfKQ4NowHlrg9IHB1tPq6YMBq5pYXBTXnMgDm1GqTrUt_9cdNk; QN267=1468374742801_fedb5877b8c42298; QN44=wskaiwx8424; _i=RBTW-L-rKGEVzH0Rs7HhIuzm4nxx; _vi=MorIFc4SD6ybzvSa95XPLJVcHYBp517iCRVDhUg6gKMHDmidTHHHGIL4mY8VjDRtOEDswc3xe6Uw-ok_wNG3949lnAQR9siYXMyswvjES5xMvvjh2b_aqPzmv0jTWalOI0pJevrCTWgwRPZ7Vfg821WfaO0U475pSuQg_DdB8W3B; RT_CACLPRICE=1; __utma=183398822.861955028.1446808053.1467428865.1468374676.8; __utmb=183398822.3.10.1468374676; __utmc=183398822; __utmz=183398822.1467428865.7.3.utmcsr=qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ag_fid=OasY1e09XySspIhF; _jzqa=1.3836438761093494300.1446808055.1467428866.1468374703.8; _jzqc=1; QN268=1468374742801_fedb5877b8c42298|1468374746181_b1aa3de059aae6a6; _jzqb=1.4.10.1468374703.1'},
        #     {'cookie': 'QN99=1305; QN1=eIQiPVeDSji00CtXghi3Ag==; QN269=D67E7050473811E6B26510517226243B; pgv_pvi=6167046144; pgv_si=s6561379328; QN70=0a3d42450155d8d9f843; QunarGlobal=10.86.213.134_6aa1a513_155d8b7f01d_4eb5|1468222012797; __ag_cm_=1468222013223; _jzqckmp=1; QN163=0; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3=1468305850; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1468305850; PHPSESSID=01586iudrc7auuk7aev5llgp72; QN73=2476-2477; flowidList=.2-2.3-1.4-1.1-3.; _jzqx=1.1468222017.1468305856.2.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/beijing_city/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/guangzhou/; QN25=14a21e39-0af1-4fbe-a509-d1c7884ff99a-9f992f90; QN271=d1f8e517-9968-4066-99b9-2c1a6fed0f73; QN42=wzzy1784; _q=U.pdacywi7180; _t=24471782; csrfToken=kcnxdjCWzdazKe5gWzQqRFejCpteqmqd; _s=s_SBJOE37D2Y7V4RU4C4FJLKFTUA; _v=NMbd0dpynyjffJF9Mt4PFY9x8BJZjvWhQouYih_AW4uyNVVSEtgjhC073dBQjxRMuelI1kUMSLFXAP-0Xyoukl8uwR-9qvolDAzi4ds82eLB3ZGt_BZgAcgyT3YpFoZtg2FJDxeFpWYMvUNGGKaTls0WCP96SbtZjkM2vtQUeCqx; ag_fid=y7C3db8YZZMxQalF; __ads_session=TLB2Y5N1wAiCp9YnogA=; __utma=183398822.532439676.1468222013.1468222013.1468305853.2; __utmb=183398822.5.10.1468305853; __utmc=183398822; __utmz=183398822.1468305853.2.2.utmcsr=qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/; _jzqa=1.2476653569659523600.1468222017.1468222017.1468305856.2; _jzqc=1; _jzqb=1.5.10.1468305856.1; QN267=1468308230390_60613c93968dd808; QN44=pdacywi7180; _i=RBTKSL-rKGExOS6xslUoh4YF43Kx; _vi=iGB7picLBP4PISiLyl09bkW3cbcu7TKqwdPWbfrooRrcw_RjSUulBobZnhDcsB_V778k2LgcMrmrAY2Sr9qJUuhaV7upx52LeKqwNPYsOtZZncHMVWW4-41YnMshAyWIn0RuF2mOfq5Z_kdMNXE1MYC4DHqBldgXWgFzFokJ27Zd; QN268=1468308230390_60613c93968dd808|1468308231720_f976758e96cf42a8; RT_CACLPRICE=1'},
        #     {'cookie': 'QN99=1069; QN1=eIQiPVeFvXBeYApZJeUNAg==; QN269=B0D7C96048AE11E69873105172262457; QN70=16e9d63ba155e26c0caf; QunarGlobal=192.168.31.103_-4c0e4afd_155e245eac0_6c98|1468382582757; pgv_pvi=9324161024; pgv_si=s3555779584; QN73=2477-2478; __ag_cm_=1468382584435; flowidList=.2-3.3-1.4-1.1-3.; _jzqckmp=1; __utmt=1; QN25=8033b9bb-e01f-447f-97a9-dc5ab8714162-9f992f90; QN271=0066eb9d-8fa5-404d-adec-d502e1fb33a1; QN43=2; QN42=wzzy1784; _q=U.pdacywi7180; _t=24582545; csrfToken=EWbn1g9f5arxnqilOXwJzKiwCh7aXFzp; _s=s_XKN373CQM2KQ7BN4CYAIIWFA5I; _v=pxhrpvkMLME-SkzUPKuEJCaPZUn_IhsxrYT3-YTmgYSDFiC6oWQy6t9CELPkrz9XdTjtFZlpWQvPGK9p4o99DfGhzj5nAXJnUouOvOgQ0dpwr-4HjhRO_kUiUAXILzx6YHgKIvXm9M5ENp7hyuCM25uqgZADiLXmd4iXs6Sespop; QN267=1468386327641_2683538e8a786ae0; QN44=pdacywi7180; RT_CACLPRICE=1; _i=RBTKSl-rKGExTYjRsldgx-Z0Um-x; _vi=5X5098PH_T1ywSfzKjl0DXViM73Iy7m1Setm23W-ORZnJWh8L1spKQjFeKKGSPHk3huR0E8x6qpfXVH3uJfiBR5flwCUKuijpBNQ5vynTblKpSd4KvpIDYDReV4Nrte7Je1kkYwzZy1FiopGwAd_Px2IbVwCDNVglHD8L-Z7WK8_; __utma=183398822.702268937.1468382584.1468382584.1468386308.2; __utmb=183398822.2.10.1468386308; __utmc=183398822; __utmz=183398822.1468386308.2.2.utmcsr=hotel.qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/city/beijing_city/; ag_fid=AqmaZkXV4klFkvzF; QN268=1468386327641_2683538e8a786ae0|1468386333915_70ac88aa03a77eb5; _jzqx=1.1468382594.1468386336.2.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/beijing_city/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/beijing_city/dt-2538/; _jzqa=1.2666420128244953600.1468382594.1468382594.1468386336.2; _jzqc=1; _jzqb=1.2.10.1468386336.1'}
        # ]
        # cookie_list = [
        #     {'cookie':'QunarGlobal=192.168.26.98_-bdac8658_f6e27baeecd_8e7c%7C1469085673285; QN1=eIQiVVeQd+k7Ua8SCbF9Ag==; QN269=B49DEB404F1311E6A493FC15B410FE08; QN70=0c5b356cc1560c5465ea; pgv_pvi=7353802752; pgv_si=s3081714688; __ag_cm_=1469085674560; QN99=3394; _jzqx=1.1469085794.1469085794.1.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/maoming/dt-2369/.-; _jzqckmp=1; QN73=2497-2498; __utmt=1; QN25=22f50664-ba55-4c4a-b5ff-6291512bba31-9f992f90; QN271=5d5c6592-517a-410c-ab9f-c9fbcadea3b7; QN42=wzzy1784; _q=U.pdacywi7180; _t=24484789; csrfToken=mkdLDvkKxkBeMzZxyzXByblU9mzv1rpf; _s=s_NTMBN2KVWR22NDPFRLBXQMUCSQ; _v=FTYVHwcYL6PG7n3StiqN9_iKdP8MpQhUF1uiqGINkXx4WEsGJ6TzCzZMiApTNhjAEKDG2_845hj1DGCc8-80Qzah2om497SldIS01YsxiCXEnBolj1Ly7XPN7WLkWgnKNlHlKaQCKhmWZzg00_FQvR6L4bTk38YPyk_Bk0FuQY7j; QN267=1469087382681_a522a16e7886813b; QN44=pdacywi7180; _i=RBTKSL-rKGExDzpxsU_s8nInDn_x; _vi=A5gtTGCS4dC9A9W7SjL3tYfFXJ7sy52YnFlSsliX8cxVsoyZeebVtgX0_kU9VPCICI7RReXmWcRkGW281nliKoiyYXIqqcakv8dlVgBClXlR8VJScVK-Knpia8TyykP9J1-yQG2FPSxvNkWp9NoaWS6-8jjaJpS3YDpWBGRcsVUU; RT_CACLPRICE=1; __utma=183398822.2017264.1469085675.1469085675.1469085675.1; __utmb=183398822.6.10.1469085675; __utmc=183398822; __utmz=183398822.1469085675.1.1.utmcsr=hotel.qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/city/maoming/dt-2369; ag_fid=sZywJViQErrMgnYF; __ads_session=DZutgnOmwQjAo/AQogA=; _jzqa=1.2030070356557397000.1469085794.1469085794.1469085794.1; _jzqc=1; _jzqb=1.8.10.1469085794.1; QN268=1469087382681_a522a16e7886813b|1469087386066_5bfb6daa9cd33bd7'}
        # ]
        if len(cookie_list) is not 0:
            cookie = random.choice(cookie_list)['cookie']
        else:
            cookie = 'QN99=1305; QN1=eIQiPVeDSji00CtXghi3Ag==; QN269=D67E7050473811E6B26510517226243B; pgv_pvi=6167046144; pgv_si=s6561379328; QN70=0a3d42450155d8d9f843; QunarGlobal=10.86.213.134_6aa1a513_155d8b7f01d_4eb5|1468222012797; __ag_cm_=1468222013223; _jzqckmp=1; QN163=0; Hm_lvt_75154a8409c0f82ecd97d538ff0ab3f3=1468305850; Hm_lpvt_75154a8409c0f82ecd97d538ff0ab3f3=1468305850; PHPSESSID=01586iudrc7auuk7aev5llgp72; QN73=2476-2477; flowidList=.2-2.3-1.4-1.1-3.; _jzqx=1.1468222017.1468305856.2.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/beijing_city/.jzqsr=hotel%2Equnar%2Ecom|jzqct=/city/guangzhou/; QN25=14a21e39-0af1-4fbe-a509-d1c7884ff99a-9f992f90; QN271=d1f8e517-9968-4066-99b9-2c1a6fed0f73; QN42=wzzy1784; _q=U.pdacywi7180; _t=24471782; csrfToken=kcnxdjCWzdazKe5gWzQqRFejCpteqmqd; _s=s_SBJOE37D2Y7V4RU4C4FJLKFTUA; _v=NMbd0dpynyjffJF9Mt4PFY9x8BJZjvWhQouYih_AW4uyNVVSEtgjhC073dBQjxRMuelI1kUMSLFXAP-0Xyoukl8uwR-9qvolDAzi4ds82eLB3ZGt_BZgAcgyT3YpFoZtg2FJDxeFpWYMvUNGGKaTls0WCP96SbtZjkM2vtQUeCqx; ag_fid=y7C3db8YZZMxQalF; __ads_session=TLB2Y5N1wAiCp9YnogA=; __utma=183398822.532439676.1468222013.1468222013.1468305853.2; __utmb=183398822.5.10.1468305853; __utmc=183398822; __utmz=183398822.1468305853.2.2.utmcsr=qunar.com|utmccn=(referral)|utmcmd=referral|utmcct=/; _jzqa=1.2476653569659523600.1468222017.1468222017.1468305856.2; _jzqc=1; _jzqb=1.5.10.1468305856.1; QN267=1468308230390_60613c93968dd808; QN44=pdacywi7180; _i=RBTKSL-rKGExOS6xslUoh4YF43Kx; _vi=iGB7picLBP4PISiLyl09bkW3cbcu7TKqwdPWbfrooRrcw_RjSUulBobZnhDcsB_V778k2LgcMrmrAY2Sr9qJUuhaV7upx52LeKqwNPYsOtZZncHMVWW4-41YnMshAyWIn0RuF2mOfq5Z_kdMNXE1MYC4DHqBldgXWgFzFokJ27Zd; QN268=1468308230390_60613c93968dd808|1468308231720_f976758e96cf42a8; RT_CACLPRICE=1'
        return cookie

