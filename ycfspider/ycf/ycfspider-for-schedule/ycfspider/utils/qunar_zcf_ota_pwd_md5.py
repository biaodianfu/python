#coding=utf-8
import json

import PyV8


class Password(object):
    #e = [1732584193, 4023233417, 2562383102, 271733878]
    def __init__(self):
        self.salt = 'Da8IVqDHomS47M9wlIn3OtyeV0xGtKxb2AWHi8YnEUCq7wYEQN2fnzszhAo8bNDE'
        self.blockSize = 512/32
        self.minBufferSize = 0

    def exec_js(self,command):
        self.get_ctx()
        #print command
        fret = self.ctx.eval("""
            function func() {
              var data = """ + command+ """;
              return data;
            }
            """)
        jsond = self.ctx.locals.func()
        return jsond
    # def parse_pwd(self, password):
    #     password = str(password)+'##'+self.salt

    #可以让一下的parse代替之
    def parse_dict(self,password):
        pwd_len = len(password)
        pwd_dict = {}
        pwd_dict['sigBytes'] = pwd_len
        n = []
        for i in range(self.init_n_len(pwd_len)):
            n.append(0)
        self.init_n_len(pwd_len)
        for i in range(pwd_len):
           n[ i >> 2 ] |= (ord(password[i])&255) << 24 - i%4 * 8
        pwd_dict['n'] = n
        return pwd_dict
    def init_n_len(self,len):
        m = []
        j = 0
        for i in range(len):
            if j < (i>>2):
                j = i>>2
        return j + 1
    # def concat(self,password):
    #     r = 0
    #     pwd_dict = self.parse_dict(password)
    #     t = pwd_dict['n']

    def doFinalize(self,pwd_dict):
        n = pwd_dict['n']
        i = pwd_dict['sigBytes'] * 8
        r = i
        n[i>>5] |= 128<<24 - i%32
        s = 0
        o = r
        new_n_len = (i + 64 >> 9 << 4) + 15 + 1
        n_len = new_n_len - len(n)
        for k in range(n_len):
            n.append(0)
        command_1 = '(('+str(s)+' << 8 |'+str(s)+'>>> 24) & 16711935 | ('+str(s)+'<< 24 |'+str(s)+' >>> 8) & 4278255360)'
        n[(i + 64 >> 9 << 4) + 15] = self.exec_js(command_1)
        command_2 = '(('+str(o)+' << 8 |'+str(o)+'>>> 24) & 16711935 | ('+str(o)+'<< 24 |'+str(o)+' >>> 8) & 4278255360)'
        n[(i + 64 >> 9 << 4) + 14] =  self.exec_js(command_2)
        pwd_dict['sigBytes'] = (len(n)+1)*4
        pwd_dict['n'] = n
        return pwd_dict
    def process(self,pwd_dict):
        an_pwd_dict = {}
        u = self.blockSize * 4
        a = self.exec_js(str(pwd_dict['sigBytes'])+'/'+str(u))
        a = self.exec_js('Math.max(('+str(a)+'| 0) - 0, 0)')
        f = a * self.blockSize
        l = self.exec_js('Math.min('+str(f * 4)+', '+str(pwd_dict['sigBytes'])+')')
        an_pwd_dict["sigBytes"] = l
        s = '[1732584193,4023233417,2562383102,271733878]'
        if(f):
           # print pwd_dict
            for i in range(0,f,self.blockSize):
                #print '-',pwd_dict['n']
                value = self.do_process_block(str(pwd_dict['n']),str(i),s)
                e =  str(value[0]['value'])
                #an_pwd_dict['n'] = e
                s = '['+ str(value[1]['value']) + ']'
                pwd_dict['n'] = e.split(',')
                #print i,e
                #print i,s
                # if i == 0:
                #     e = '['+ str(self.do_process_block(str(pwd_dict['n']),str(i),s)[0]['value']) + ']'
                #     an_pwd_dict['n'] = e
                #     s_content = '['+ str(self.do_process_block(str(pwd_dict['n']),str(i),s)[1]['value']) + ']'
                #     s_content = json.loads(s_content)
                #     print s_content
                #    # print str(self.do_process_block(str(pwd_dict['n']),str(i),s)[2]['value'])
                # else:
                #     u = '['+ str(self.do_process_block(str(pwd_dict['n']),str(i),str(s_conten))[1]['value']) + ']'
                #     #print u

        e_pwd_dict = {}
        e_pwd_dict["sigBytes"] = 4#pwd_dict['sigBytes']此时应该设置为0，此时n为空
        e_pwd_dict['n']=[]
        return s
    def do_finalize_u(self,u):
        for i in range(4):
            l = u[i]
            u[i] =self.exec_js('('+str(l)+' << 8 |'+ str(l) +'>>> 24) & 16711935 | ('+str(l)+'<< 24 |'+ str(l)+'>>> 8) & 4278255360')
        return u

    def do_process_block(self,e,i,s):
        self.get_ctx()
        #print command
        fret = self.ctx.eval("""
            function func() {
                function f(e, t, n, r, i, s, o) {
                    var u = e + (t & n | ~t & r) + i + o;
                    return (u << s | u >>> 32 - s) + t
                }
                function l(e, t, n, r, i, s, o) {
                    var u = e + (t & r | n & ~r) + i + o;
                    return (u << s | u >>> 32 - s) + t
                }
                function c(e, t, n, r, i, s, o) {
                    var u = e + (t ^ n ^ r) + i + o;
                    return (u << s | u >>> 32 - s) + t
                }
                function h(e, t, n, r, i, s, o) {
                    var u = e + (n ^ (t | ~r)) + i + o;
                    return (u << s | u >>> 32 - s) + t
                }
                var u = [];
                var arr = [];
                var e = """+e+""";
                for (var x = 0; x < 64; x++) {
                        u[x] = Math.abs(Math.sin(x + 1)) * 4294967296 | 0;
                    }
                for (var n = 0; n < 16; n++) {
                            var r = """+i+""" + n;
                            var i = e[r];
                            e[r] = (i << 8 | i >>> 24) & 16711935 | (i << 24 | i >>> 8) & 4278255360
                        }
                        var t = """+i+""";
                        var s = """+s+""";
                        var o = e[t+ 0];
                        var a = e[t+1];
                        var p = e[t + 2];
                        var d = e[t + 3];
                        var v = e[t + 4];
                        var m = e[t + 5];
                        var g = e[t + 6];
                        var y = e[t+ 7];
                        var b = e[t+ 8];
                        var w = e[t+ 9];
                        var E = e[t+ 10];
                        var S = e[t+ 11];
                        var x = e[t+ 12];
                        var N = e[t + 13];
                        var C = e[t + 14];
                        var k = e[t + 15];
                        var L = s[0];
                        var A = s[1];
                        var O = s[2];
                        var M = s[3];
                        L = f(L, A, O, M, o, 7, u[0]);
                        M = f(M, L, A, O, a, 12, u[1]);
                        O = f(O, M, L, A, p, 17, u[2]);
                        A = f(A, O, M, L, d, 22, u[3]);
                        L = f(L, A, O, M, v, 7, u[4]);
                        M = f(M, L, A, O, m, 12, u[5]);
                        O = f(O, M, L, A, g, 17, u[6]);
                        A = f(A, O, M, L, y, 22, u[7]);
                        L = f(L, A, O, M, b, 7, u[8]);
                        M = f(M, L, A, O, w, 12, u[9]);
                        O = f(O, M, L, A, E, 17, u[10]);
                        A = f(A, O, M, L, S, 22, u[11]);
                        L = f(L, A, O, M, x, 7, u[12]);
                        M = f(M, L, A, O, N, 12, u[13]);
                        O = f(O, M, L, A, C, 17, u[14]);
                        A = f(A, O, M, L, k, 22, u[15]);
                        L = l(L, A, O, M, a, 5, u[16]);
                        M = l(M, L, A, O, g, 9, u[17]);
                        O = l(O, M, L, A, S, 14, u[18]);
                        A = l(A, O, M, L, o, 20, u[19]);
                        L = l(L, A, O, M, m, 5, u[20]);
                        M = l(M, L, A, O, E, 9, u[21]);
                        O = l(O, M, L, A, k, 14, u[22]);
                        A = l(A, O, M, L, v, 20, u[23]);
                        L = l(L, A, O, M, w, 5, u[24]);
                        M = l(M, L, A, O, C, 9, u[25]);
                        O = l(O, M, L, A, d, 14, u[26]);
                        A = l(A, O, M, L, b, 20, u[27]);
                        L = l(L, A, O, M, N, 5, u[28]);
                        M = l(M, L, A, O, p, 9, u[29]);
                        O = l(O, M, L, A, y, 14, u[30]);
                        A = l(A, O, M, L, x, 20, u[31]);
                        L = c(L, A, O, M, m, 4, u[32]);
                        M = c(M, L, A, O, b, 11, u[33]);
                        O = c(O, M, L, A, S, 16, u[34]);
                        A = c(A, O, M, L, C, 23, u[35]);
                        L = c(L, A, O, M, a, 4, u[36]);
                        M = c(M, L, A, O, v, 11, u[37]);
                        O = c(O, M, L, A, y, 16, u[38]);
                        A = c(A, O, M, L, E, 23, u[39]);
                        L = c(L, A, O, M, N, 4, u[40]);
                        M = c(M, L, A, O, o, 11, u[41]);
                        O = c(O, M, L, A, d, 16, u[42]);
                        A = c(A, O, M, L, g, 23, u[43]);
                        L = c(L, A, O, M, w, 4, u[44]);
                        M = c(M, L, A, O, x, 11, u[45]);
                        O = c(O, M, L, A, k, 16, u[46]);
                        A = c(A, O, M, L, p, 23, u[47]);
                        L = h(L, A, O, M, o, 6, u[48]);
                        M = h(M, L, A, O, y, 10, u[49]);
                        O = h(O, M, L, A, C, 15, u[50]);
                        A = h(A, O, M, L, m, 21, u[51]);
                        L = h(L, A, O, M, x, 6, u[52]);
                        M = h(M, L, A, O, d, 10, u[53]);
                        O = h(O, M, L, A, E, 15, u[54]);
                        A = h(A, O, M, L, a, 21, u[55]);
                        L = h(L, A, O, M, b, 6, u[56]);
                        M = h(M, L, A, O, k, 10, u[57]);
                        O = h(O, M, L, A, g, 15, u[58]);
                        A = h(A, O, M, L, N, 21, u[59]);
                        L = h(L, A, O, M, v, 6, u[60]);
                        M = h(M, L, A, O, S, 10, u[61]);
                        O = h(O, M, L, A, p, 15, u[62]);
                        A = h(A, O, M, L, w, 21, u[63]);
                        s[0] = s[0] + L | 0;
                        s[1] = s[1] + A | 0;
                        s[2] = s[2] + O | 0;
                        s[3] = s[3] + M | 0
                    arr.push({
                    'key': 'e',
                    'value': e,
                });arr.push({
                    'key': 's',
                    'value': s,
                });arr.push({
                    'key': 'u',
                    'value': u,
                });
                return arr;
            }
            """)
        jsond = self.ctx.locals.func()
        return jsond

    def get_ctx(self):
        self.ctx = PyV8.JSContext()
        self.ctx.enter()

    #e = init {words: Array[4], sigBytes: 16}
    def stringify(self,e,sigBytes):
        r = self.exec_js_for(str(e),16)
        return str(r).replace(',','')
    def exec_js_for(self,e,n):
        self.get_ctx()
        #print command
        fret = self.ctx.eval("""
            function func() {
               var t = """+e+""";
               var r = [];
               for (var i = 0; i <"""+str(n)+""" ; i++) {
                            var s = t[i >>> 2] >>> 24 - i % 4 * 8 & 255;
                            r.push((s >>> 4).toString(16));
                            r.push((s & 15).toString(16))
                        }
              return r;
            }
            """)
        jsond = self.ctx.locals.func()
        return jsond

    #以上为password的MD5加密，下面为password_md5+usname+cookies的加密
    def parse_user_cookie_md5(self,pwd_md5,username,cookie):
        pwd_dict = {}
        e =str(pwd_md5)+'#'+str(username)+'#'+str(cookie)
        pwd_dict['sigBytes'] = len(e)
        parse_result = self.parse(e)
        pwd_dict['n'] = json.loads('['+str(parse_result)+']')
       # print '+',pwd_dict['n']
        new_dict =  self.doFinalize2(pwd_dict)
        doFinalize2_result = str(new_dict[0]['value']).split(",")
        process_dict = {}
        process_dict['sigBytes'] = new_dict[1]['value']
        process_dict['n'] = doFinalize2_result
        #print process_dict
        u =  self.process(process_dict)
        stringify_e = self.do_finalize_u(json.loads(u))
        return self.stringify(stringify_e,16)
    def parse(self,e):
        self.get_ctx()
        n = len(e)
        fret = self.ctx.eval("""
            function func() {
                        var t = """+str(n)+""";
                        var n = [];
                        for (var r = 0; r < t; r++) {
                            n[r >>> 2] |= ('"""+str(e)+"""'.charCodeAt(r) & 255) << 24 - r % 4 * 8
                        }
                return n;
            }""")
        jsond = self.ctx.locals.func()
        return jsond

    def doFinalize2(self,pwd_dict):
        self.get_ctx()
        n = pwd_dict['n']
        sigBytes = pwd_dict['sigBytes']
        #print command
        fret = self.ctx.eval("""
            function func() {
                        var e = '"""+str(n)+"""';
                        var arr = [];
                        var n = """+str(n)+""";
                        var r = """+str(sigBytes)+""" * 8;
                        var i = """+str(sigBytes)+"""  * 8;
                        n[i >>> 5] |= 128 << 24 - i % 32;
                        var s = Math.floor(r / 4294967296);
                        var o = r;
                        n[(i + 64 >>> 9 << 4) + 15] = (s << 8 | s >>> 24) & 16711935 | (s << 24 | s >>> 8) & 4278255360;
                        n[(i + 64 >>> 9 << 4) + 14] = (o << 8 | o >>> 24) & 16711935 | (o << 24 | o >>> 8) & 4278255360;
                        sigBytes = (n.length + 1) * 4;
                        arr.push({
                    'key': 'n',
                    'value': n,
                });   arr.push({
                    'key': 'sigBytes',
                    'value': sigBytes,
                });
                return arr;
            }""")
        jsond = self.ctx.locals.func()
        return jsond

    def parse_password_md5(self,pwd):
        password = str(pwd)+'#'+ self.salt
        n = self.parse_dict(password)
        arr = self.doFinalize2(n)
        process_dict = {}
        process_dict["sigBytes"] = arr[1]['value']
        process_dict['n'] = str(arr[0]['value']).split(',')
       # print process_dict
        password_md5 = self.stringify(self.do_finalize_u(json.loads(self.process(process_dict))),16)
        return password_md5
#a935edf2e33c6c81bc1018a7898125b8
#a935edf2e33c6c81bc1018a7898125b8#kefu1#OWQ3ZmQ3NWY2ODg1YmU5ZjdiMmIyYmM1NjAwMjNlOTEzZTk0NWVmNDlkNjM3NGEzZjRlMDIzMWRlNzRkZGE4MyMxNDcxMzExNzU5OTAyIzEzNjM0NTExOTAjcmhzdm1I
if __name__ == "__main__":
    qunar_login_header_1 = {
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Host':'cdycf.zcfgoagain.com',
        'Proxy-Connection':' keep-alive',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest'
    }
    qunar_login_header_2 = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Content-Type':'application/json;charset=UTF-8',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        'X-Requested-With':' XMLHttpRequest',
    }
    cookie = 'OWVjYTJjZDFmODdmMDhmMWQzYmFhNzE2NTUwZTRjYjExZDk2YzI0NTk2ODcxMzFiNmNkMmM4NGJlOThmMTllZCMxNDcxODYzMzE1ODUzIzE5MDMwMzY2OTAjZnEwYmtG'
    password = Password()
    password_md5 = password.parse_password_md5('ycf123456')
    # print password_md5
    # print password.parse_user_cookie_md5(password_md5,'kefu2',cookie)
    # url = 'http://cdycf.zcfgoagain.com/rbac/api/usr/getLoginToken?userName=readgo&_time=' + str(int(time.time() * 1000))
    # json_data = requests.get(url, headers=qunar_login_header_1, timeout=5).json()
    # data = json_data['data']
    # password_md5 = password.parse_password_md5('yaochufa123')
    # pwd = password.parse_user_cookie_md5(password_md5,'readgo',data)
    # print pwd
    # qunar_login_header_2['Cookie'] = 'gaultk='+data
    # print pwd
    # print pwd
    # cookies = {'_sq': 'ha3sq%3Aadmin', ' _sv': 'NQD32cGxkRks4cl7DZMKN3QlLkJCx8nCyQbIHSIVf5B7aMmS9YoMQVA8J9EkY9YJArVNxWzXDYCsp7Fkuamz3p1SSJEydtJqEhzu00YANouVSXxhqiqn3cUDfubP-Trs53aD1GCDnzrhnj9SA9fngFWLAcqJ4eWd7Fb0JkzfUO2J', ' _st': '1471604184572', ' JSESSIONID': '2264E5582B5063246AFA971780B91246'}
    # #body = {'currentPage': 1, 'pageSize': 1000, 'shotelId': 2453525, 'bizTypeSet': []}
    # print requests.post(url='http://hota.qunar.com/price/oapi/rateplan/detail/2760635', headers=qunar_login_header_2, timeout=5,cookies=cookies).content




