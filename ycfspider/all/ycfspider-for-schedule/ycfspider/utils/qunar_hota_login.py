# -*- coding: utf-8 -*-
import redis
# import pymysql

from ycfspider.utils.login_utils import LoginUtil
from scrapy.conf import settings


class QunarHotaLogin():

    def __init__(self):
        self.redis_key =  'login:qunar_hota_login'
        self.client = redis.Redis(host=settings.get('REDIS_HOST'),port=settings.get('REDIS_PORT'),db=0)
        self.login_util = LoginUtil()
        # self.collection=pymysql.connect(host=settings.get('LOGIN_MYSQL_HOST'),port=settings.get('LOGIN_MYSQL_PORT'),user=settings.get('LOGIN_MYSQL_USERNAME'),passwd=settings.get('LOGIN_MYSQL_USERPWD'),db=settings.get('LOGIN_MYSQL_DB'),charset='utf8')

    def get_user_cookie(self,user_key):
        hexists = self.client.hexists(self.redis_key,user_key)
        if hexists:
            return self.client.hget(self.redis_key,user_key)
        else:
            return 0

    def set_user_cookie(self,user_key,cookie):
        #cookie是一个username:cookie的dict
        hexists = self.client.hexists(self.redis_key,user_key)
        if hexists :
            self.client.hdel(self.redis_key,user_key)
        if self.client.hset(self.redis_key,user_key,cookie):
            return 1
        else:
            return 0

    def login(self,user_name,pwd,channel_number):
        #连接数据库，查询对应的用户，并进行校验，找到用户的密码
       # user_cur = self.collection.cursor()
        #sel_user_message = "select * from "+settings.get('LOGIN_MYSQL_TABLE')+" where LoginAccount = '"+user_name+"';"
        # user_cur.execute(sel_user_message)
        # user_info = user_cur.fetchall()
        # user_dict = {}
        # print len(user_info)
        # print user_info
        # if len(user_info) == 1:
        #     for item in user_info:
        #         user_dict['channel_id'] = item[0]
        #         user_dict['region'] = item[5]
        #         user_dict['channel_number'] = item[1]
        #         user_dict['login_url'] = item[3]
        #         user_dict['user_name'] = item[6]
        #         user_dict['password'] = item[7]
        # user_cur.close()
        # self.collection.close()
        # print user_dict
        # if  user_dict.get('password'):
        #pwd = user_dict.get('password')
        #cookies = {'user_name':user_name,'cookie':''}
        cookie = self.login_util.qunar_hota_login(user_name,pwd)
        if cookie:
            if self.set_user_cookie(user_name+'_'+channel_number,str(cookie)):
                return str(cookie)
            else:
                return None
        else:
            return None

if __name__ == "__main__":
    pass
    q = QunarHotaLogin()
    q.login(str(111112))
    # q.get_user_pwd('user_name_and_id','testpwd')
    # q.get_user_pwd('user_name_and_id','testpwd2')
    # client = redis.Redis(host=settings.get('REDIS_HOST'),port=settings.get('REDIS_PORT'),db=0)
    # print "Connection to server successfully!"
    # dicKeys = client.keys("*")
    #print dicKeys

    ### Redis hash command part Start ###
    # hset: Set key to value with hash name,hset(self, name, key, value)
    # hget: Return the value of ``key`` within the hash ``name``, hget(self, name, key)
    # client.hset('myhash','field1',"foo")
    # hashVal = client.hget('myhash','field1')
    # print "Get hash value:",hashVal

    # # Get none-value
    # hashVal = client.hget('myhash','field2')
    # print "None hash value:",hashVal
    #
    # # hexists: Returns a boolean indicating if ``key`` exists within hash ``name``
    # keyList= ['field1','field2']
    # for key in keyList:
    #     hexists = client.hexists('myhash',key)
    #     if hexists :
    #         print "Exist in redis-hash key:",key
    #     else:
    #         print "Not exist in redis-hash key:",key

    # # hgetall: Return a Python dict of the hash's name/value pairs
    # client.hset('myhash','field2',"bar")
    # valDict = client.hgetall('myhash')
    # print "Get python-dict from redis-hash",valDict
    #
    # # hincrby: Increment the value of ``key`` in hash ``name`` by ``amount``
    # # default increment is 1,
    # client.hset('myhash','field',20)
    # client.hincrby('myhash','field')
    # print "Get incrby value(Default):",client.hget('myhash','field')
    # client.hincrby('myhash','field',2)
    # print "Get incrby value(step: 2):",client.hget('myhash','field')
    # client.hincrby('myhash','field',-3)
    # print "Get incrby value(step: -3):",client.hget('myhash','field')
    #
    # # no method hincrbyfloat
    #
    # #hkeys: Return the list of keys within hash ``name``
    # kL = client.hkeys('myhash')
    # print "Get redis-hash key list",kL
    #
    # #hlen: Return the number of elements in hash ``name``
    # lenHash =client.hlen('myhash')
    # print "All hash length:",lenHash
    #
    # #hmget: Returns a list of values ordered identically to ``keys``
    # #hmget(self, name, keys), keys should be python list data structure
    # val =client.hmget('myhash',['field','field1','field2','field3','fieldx'])
    # print "Get all redis-hash value list:",val
    #
    # #hmset:  Sets each key in the ``mapping`` dict to its corresponding value in the hash ``name``
    # hmDict={'field':'foo','field1':'bar'}
    # hmKeys=hmDict.keys()
    # client.hmset('hash',hmDict)
    # val = client.hmget('hash',hmKeys)
    # print "Get hmset value:",val
    #
    # #hdel: Delete ``key`` from hash ``name``
    # client.hdel('hash','field1')
    # print "Get delete result:",client.hget('hash','field')

    # #hvals:  Return the list of values within hash ``name``
    # val = client.hvals('myhash')
    # print "Get redis-hash values with HVALS",val

    # #hsetnx: Set ``key`` to ``value`` within hash ``name`` if ``key`` does not exist.
    # #      Returns 1 if HSETNX created a field, otherwise 0.
    # r=client.hsetnx('myhash','field',2)
    # print "Check hsetnx execute result:",r," Value:",client.hget('myhash','field')
    # r=client.hsetnx('myhash','field10',20)
    # print "Check hsetnx execute result:",r,"Value",client.hget('myhash','field10')
    #
    # hashVal = client.hgetall('profile')
    # print hashVal
    # #Empty db
    # client.flushdb()

