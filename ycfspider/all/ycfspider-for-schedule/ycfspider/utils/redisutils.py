#coding=utf-8
__author__ = 'lizhipeng'

import redis

class RedisUtil(object):

    def __init__(self, host, port=6379, db=0):
        self.r = redis.StrictRedis(host=host, port=int(port), db=int(db))

    def pop(self, key):
        return self.r.lpop(key)

    def push(self, key, value):
        self.r.lpush(key, value)

    def len(self, key):
        return self.r.llen(key)

    def get_all(self, key):
        k_len = self.r.llen(key)
        return self.r.lrange(key, 0, k_len)

    def delete(self, key):
        self.r.delete(key)

    def hget(self, key, h_key):
        # key = 'productPrice:%s' % key
        return self.r.hget(key, h_key)

    def hset(self, key, h_key, value):
        # key = 'productPrice:%s' % key
        return self.r.hset(key, h_key, value)

    # 自增
    def incr(self, key, default=1):
        # key = 'productNum:%s' % key
        if (1 == default):
            return self.r.incr(key)
        else:
            return self.r.incr(key, default)

    def decr(self, key, default=1):
        if (1 == default):
            return self.r.decr(key)
        else:
            return self.r.decr(key, default)

    def get(self, key):
        count  = self.r.get(key)
        if count is None:
            count = 0
        return int(count)

    #查询键（key中可包含匹配符）
    def check_key(self, key):
        return self.r.keys(key)

# r = RedisUtil()

if __name__ == "__main__":
    # print r.push('task:1', 3)
    id_list = r.get_all('task:1')
    if str(3) in id_list:
        print id_list