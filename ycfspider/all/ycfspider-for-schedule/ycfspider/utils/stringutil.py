#coding=utf-8
__author__ = "linbingfeng"

#利用Unicode编码，判断一个字符串类型，返回一个布尔值
class StringUtil():
    @classmethod
    def is_chinese(cls,uchar):
        """判断一个unicode是否是汉字"""
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False

    @classmethod
    def is_number(cls,uchar):
        """判断一个unicode是否是数字"""
        if uchar >= u'\u0030' and uchar <= u'\u0039':
            return True
        else:
            return False

    @classmethod
    def is_chinese_symbol(cls, uchar):
        """判断一个unicode是否是常用中文符号"""
        #常用中文符号的Unicode编码列表
        symbol_list = [u'\u00b7',u'\u00d7',u'\u2014',u'\u2018',u'\u2019',u'\u201c',u'\u201d',u'\u2026',u'\u3001',u'\u3002',u'\u300a',u'\u300b',u'\u300e',u'\u300f',u'\u3010',u'\u3011',u'\uff01',u'\uff08',u'\uff09',u'\uff0c',u'\uff1a',u'\uff1b',u'\uff1f']
        for i in symbol_list:
            if uchar == i:
                return True
            else:
                pass
        return False

    @classmethod
    def is_alphabet(cls,uchar):
        """判断一个unicode是否是英文字母"""
        if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
            return True
        else:
            return False

    @classmethod
    def is_other(cls,uchar):
        #判断是否非汉字，数字和英文字符和斜线"/"
        if not (StringUtil.is_chinese(uchar) or StringUtil.is_number(uchar) or StringUtil.is_alphabet(uchar)):
            if(uchar == "/"):
                return  False
            return True
        else:
            return False