#encoding=utf-8
__author__ = 'lizhipeng'

from pymongo import MongoClient
from scrapy.conf import settings
from scrapy.exceptions import DropItem
from scrapy import log
import datetime

from price_listen.items import ElongHotelInfoItem
import json
import requests
from price_listen.utils.model_logger import Logger

class MongoDBPipeline( object ):

   def __init__( self ):
     client = MongoClient(settings[ 'MONGODB_HOST' ], settings[ 'MONGODB_PORT' ])
     db = client[settings[ 'MONGODB_DATABASE' ]]
     self .collection = db[settings[ 'MONGODB_COLLECTION' ]]


   def process_item(self, item, spider):
     valid = True
     for data in item:
       if not data:
         valid = False
         raise DropItem( "Missing {0}!" . format (data))

     if valid:
       if isinstance(item, ElongHotelInfoItem):
           self.collection.insert(dict(item))

       log.msg( "add mongodb success:"+item['results']['hotel_id'] + ' ' + item['results']['hotel_name'],
           level = log.INFO, spider = spider)
     return ''

