#encoding=utf-8
import smtplib
from email.mime.text import MIMEText
from email.header import Header

__author__ = 'lizhipeng'


class EmailServer(object):

    def send_email(self, receivers, msg):
        sender = 'lizhipeng@yaochufa.com'
        subject = '爬虫监控预警'
        smtpserver = 'smtp.exmail.qq.com'
        username = 'lizhipeng@yaochufa.com'
        password = '3875168lzP'

        msg = MIMEText(msg, 'plain', 'utf-8')
        msg['From'] = Header("爬虫监控预警中心", 'utf-8')
        # msg['To'] =  Header("测试", 'utf-8')

        msg['Subject'] = subject

        smtp = smtplib.SMTP()
        smtp.connect(smtpserver)
        smtp.login(username, password)
        smtp.sendmail(sender, receivers, msg.as_string())
        smtp.quit()
        # print u'email发送成功'
server = EmailServer()
# server.send_email(['641785844@qq.com', 'lizhipeng@yaochufa.com'], 'mongodb报错')
