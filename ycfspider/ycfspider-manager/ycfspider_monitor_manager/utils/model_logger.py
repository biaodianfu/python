#!/usr/bin/env
# coding:utf-8
import logging
import logging.handlers
import time
import os

class Logger(logging.Logger):

    def __init__(self, filename=None):
        super(Logger, self).__init__(self)
        # 日志文件名
        if filename is None:
            filename = 'my.log'
        if not os.path.exists(filename):
            os.makedirs(filename)
        self.filename = filename+'/'+str(int(time.time()))

        # 创建一个handler，用于写入日志文件 (每天生成1个，保留7天的日志)
        fh = logging.handlers.TimedRotatingFileHandler(self.filename, 'midnight', 1, 7)
        fh.suffix = "%Y%m%d.log"
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s[line:%(lineno)d] - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.addHandler(fh)
        self.addHandler(ch)

if __name__ == '__main__':
    logger_data = Logger('client_logs')
    for i in range(0, 10):
        logger_data.info('Test')
        sleep(1)