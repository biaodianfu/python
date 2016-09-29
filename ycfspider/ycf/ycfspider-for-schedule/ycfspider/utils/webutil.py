#coding=utf-8
__author__ = "linbingfeng"

from scrapy.selector import Selector
from ycfspider.utils.stringutil import StringUtil

class WebUtil():

    #利用xpath表达式，获取解析表达式内的文本信息（去掉标签），返回一个字符串
    @classmethod
    def get_web_static_info(cls,response,xpath):
            #data_xpath = '//div[@class="feature-traffic"]'
            data = ""
            sel =  Selector(response)
            data_list = sel.xpath(xpath).extract()
            data_string = ""
            data_list_del_tag = []
            if len(data_list) > 0:
                for string in data_list:
                    data_string += string.strip()
            data_list = data_string.split(">")
            for string in data_list:
                data_list_del_tag.append(string.strip())
            data_list[:] = []
            for string in data_list_del_tag:
                if string[0:1] == "<":
                    pass
                else:
                    data_list.append(string)
            data_string = "#".join(data_list)
            data_list_del_tag = data_string.split("<")
            data_string = "#".join(data_list_del_tag)
            data_list_del_tag = data_string.split("#")
            data_list[:] = []
            for string in data_list_del_tag:
                if StringUtil.is_chinese(string[0:1]) or StringUtil.is_number(string[0:1]) or StringUtil.is_chinese_symbol(string[0:1]) or StringUtil.is_other(string[0:1]):
                    data_list.append(string)
                else:
                    pass
            for string in data_list:
                data = data + string.strip() + "\t"
            return data