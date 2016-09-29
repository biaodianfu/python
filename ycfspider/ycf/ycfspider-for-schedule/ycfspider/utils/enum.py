__author__ = 'lizhipeng'

def enum(**enums):
    return type('Enum', (), enums)

ChannelEnum = enum(CTRIP=1, QUNAR=2, ELONG=3, MEITUAN=4, LVMAMA=7)
PlatformEnum = enum(PC=1, APP=2, M=3)
CrawlTypeEnum = enum(HOTELINFO=1, HOTELPRICE=2, SCENICINFO=3, SCENICPIRCE=4, OTAHOTELINFOANDPRICE=5)
ErrorTypeEnum = enum(PARSEERROR=1, DOWLOADERROR=2, LOGICERROR=3)