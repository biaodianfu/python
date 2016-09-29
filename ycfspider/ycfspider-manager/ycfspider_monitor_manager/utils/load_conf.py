#encoding=utf-8

import os
import ConfigParser

__author__ = 'lizhipeng'


class LoadConf(object):


    def load(self):
        current_path = os.getcwd()
        parent_path = os.path.dirname(current_path)
        conf_path = parent_path.replace('\\', '/') + '/conf/manager.conf'
        cf = ConfigParser.ConfigParser()
        cf.read(conf_path)
        return cf
load_conf = LoadConf()
# cf = load_conf.load()
# print cf.get('redis', 'redis_host')