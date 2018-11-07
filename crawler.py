
import time
from multiprocessing import Pool
import logging

import requests
import lxml.html
import time
import re
from lxml.html.soupparser import fromstring
from lxml import etree
import sys
import signal

import global_vars
from logger import logger_worker_init, logger_init
from user_agent import UserAgent
from url_utils import slash_join


# --------------------------------------------------------------------------------------------------
class Crawler(object):

    def __init__(self, **kwargs):

        self.proxy_db_conn = None
        self.parser_db_conn = None
        self.num_proc = int(kwargs.get('num_proc'))

        self.cookies = dict()
        self.http_timeout = 12
        self.alarm_timeout = self.http_timeout + 6
        self.main_host = kwargs.get('main_host')
        self.cdsk_cookie = None
        self.phpsessid_cookie = None
        self.req_count = 0
        self.max_requsets_per_proxy = 12



    # ----------------------------------------------------------------------------------------------
    def saveSessionIDs(self):
        if isinstance(self.cookies, dict):
            if 'PHPSESSID' in self.cookies:
                self.phpsessid_cookie = self.cookies['PHPSESSID']

            if 'cdsk' in self.cookies:
                self.cdsk_cookie = self.cookies['cdsk']

        return

    # ----------------------------------------------------------------------------------------------
    def restoreSessionIDs(self):
        if self.phpsessid_cookie:
            if isinstance(self.cookies, dict):
                self.cookies['PHPSESSID'] = self.phpsessid_cookie

        if self.cdsk_cookie:
            if isinstance(self.cookies, dict):
                self.cookies['cdsk'] = self.cdsk_cookie

        return


    # ----------------------------------------------------------------------------------------------
    def makeRequestInitCookies(self):

        headers = {
            'User-Agent': "",
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            'Accept-Language': "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            'Referer': self.main_host,
        }
        self.ua = self.u_agent.random_agent()
        headers['User-Agent'] = self.ua

        sess = requests.Session()

        signal.signal(signal.SIGALRM, _timeout)
        signal.alarm(self.alarm_timeout)

        try:
            response = sess.get(self.main_host, timeout=self.http_timeout, headers=headers, proxies=self.proxy_list, verify=True)
            # response = sess.get(self.main_host, timeout=self.http_timeout, headers=headers, proxies=dict(https=socks5), verify=True)
            print("makeRequestInitCookies: cookies 1 => ", self.cookies)
            try:  self.cookies = self.cookies.update(sess.cookies.get_dict())
            except AttributeError:   self.cookies = sess.cookies.get_dict()
            self.saveSessionIDs()
            print("makeRequestInitCookies: cookies 2 => ", self.cookies)

        except requests.RequestException as err:
            signal.alarm(0)
            global_vars.main_logger.error("Crawler::makeRequestInitCookies: Network error = " + str(err))
            print("Crawler::makeRequestInitCookies: Network error = " + str(err))
            # self.changeProxy()

        except TimeoutException:
            signal.alarm(0)
            global_vars.main_logger.error("Crawler::makeRequestInitCookies: Alarm Timeout")

        except requests.exceptions.ReadTimeout as err:
            signal.alarm(0)
            global_vars.main_logger.error("Crawler::makeRequestInitCookies: ReadTimeout = " + str(err))
            print("Crawler::makeRequestInitCookies: ReadTimeout = " + str(err))

        except requests.exceptions.ProxyError as err:
            signal.alarm(0)
            global_vars.main_logger.error("Crawler::makeRequestInitCookies: ProxyError = " + str(err))
            print("Crawler::makeRequestInitCookies: ProxyError = " + str(err))

        except requests.exceptions.ReadTimeout as err:
            signal.alarm(0)
            global_vars.main_logger.error("Crawler::makeRequestInitCookies: ReadTimeout  = " + str(err))
            print("Crawler::makeRequestInitCookies: ReadTimeout  = " + str(err))

        else:
            signal.alarm(0)

        self.req_count = self.req_count + 1

        return


    # ----------------------------------------------------------------------------------------------
    def makeRequest(self, url):
        print("=============================== makeRequest: url = " + url, " proxy => ", self.proxy_list)

        signal.signal(signal.SIGALRM, _timeout)
        signal.alarm(self.alarm_timeout)

        try:
            headers = {
                'User-Agent': "",
                'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                'Accept-Language': "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            }

            if not self.ua:
                self.ua = self.u_agent.random_agent()
            headers['User-Agent'] = self.ua
            sess = requests.Session()

            self.req_count = self.req_count + 1
            if self.req_count > self.max_requsets_per_proxy:
                self.changeProxy()

            try:
                self.restoreSessionIDs()
                response = sess.get(url, timeout=self.http_timeout, headers=headers, cookies=self.cookies, proxies=self.proxy_list, verify=True)
                # response = sess.get(url, timeout=self.http_timeout, headers=headers, proxies=dict(https=socks5), verify=True)
                print("makeRequest: cookies 1 => ", self.cookies)
                try:  self.cookies = self.cookies.update(sess.cookies.get_dict())
                except AttributeError:   self.cookies = sess.cookies.get_dict()
                self.saveSessionIDs()
                print("makeRequest: cookies 2 => ", self.cookies)

            except requests.exceptions.SSLError as err:
                global_vars.main_logger.error("Crawler::makeRequest: SSLError = " + str(err))
                try:
                    self.restoreSessionIDs()
                    response = sess.get(url, timeout=self.http_timeout, headers=headers, cookies=self.cookies, proxies=self.proxy_list, verify=False)
                    # response = sess.get(url, timeout=self.http_timeout, headers=headers, proxies=dict(https=socks5), verify=True)
                    print("makeRequest: cookies (2nd try) 1 => ", self.cookies)
                    try:  self.cookies = self.cookies.update(sess.cookies.get_dict())
                    except AttributeError:   self.cookies = sess.cookies.get_dict()
                    self.saveSessionIDs()
                    print("makeRequest: cookies (2nd try) 2 => ", self.cookies)

                except Exception as err:
                    signal.alarm(0)
                    global_vars.main_logger.error("Crawler::makeRequest: SSL unexpected error = " + str(err))
                    return None, 0

            except requests.RequestException as err:
                signal.alarm(0)
                global_vars.main_logger.error("Crawler::makeRequest: Network error = " + str(err))
                print("Crawler::makeRequest: Network error = " + str(err))
                # self.changeProxy()
                return None, 0

            except requests.exceptions.ReadTimeout as err:
                signal.alarm(0)
                global_vars.main_logger.error("Crawler::makeRequest: ReadTimeout = " + str(err))
                print("Crawler::makeRequest: ReadTimeout = " + str(err))
                return None, 0

            except requests.exceptions.ProxyError as err:
                signal.alarm(0)
                global_vars.main_logger.error("Crawler::makeRequest: ProxyError = " + str(err))
                print("Crawler::makeRequest: ProxyError = " + str(err))
                return None, 0

            except requests.exceptions.ReadTimeout as err:
                signal.alarm(0)
                global_vars.main_logger.error("Crawler::makeRequest: ReadTimeout  = " + str(err))
                return None, 0

            else:
                signal.alarm(0)
                return response, response.status_code

        except TimeoutException:
            signal.alarm(0)
            del sess
            global_vars.main_logger.error("Crawler::makeRequest: Alarm Timeout")
            return None, 0


    # -----------------------------------------------------------------------------------------------
    def detectBan(self, content_str):
        # print(content_str)
        if content_str.find("recaptcha") != -1 or content_str.find("превышена допустимая интенсивность запросов") != -1:
            print("detectBan: --------------> DETECTED BAN...")
            return True
        else:
            return False


    # -----------------------------------------------------------------------------------------------
    def markProxyBanned(self):
        # print("================================= markProxyBanned: host, port = ", self.proxy_host, self.proxy_port)
        proxy_sql = """ UPDATE  proxy_host  SET  is_banned = 1, banned_dt = now()  WHERE  proxy_ip = %s  AND  proxy_port = %s """

        try:
            proxy_cur = self.proxy_dbh.cursor()
            proxy_cur.execute(proxy_sql, (self.proxy_host, self.proxy_port))
            proxy_cur.close()
            self.proxy_dbh.commit()
            del proxy_cur
        except Exception as e:
            print("markProxyBanned: Unexpected exception = ", e)
            pass

        return


    # -----------------------------------------------------------------------------------------------
    def changeProxy(self):
        proxy_sql = """ SELECT  proxy_ip, proxy_port  FROM  proxy_host  WHERE  is_banned <> 1  ORDER BY  rand()  LIMIT  1 """
        proxy_ip, proxy_port = None, None

        try:
            proxy_cur = self.proxy_dbh.cursor()
            proxy_cur.execute(proxy_sql)
            proxy_ip, proxy_port = proxy_cur.fetchone()
            proxy_cur.close()
            del proxy_cur

            self.proxy_list = {
                'http': "http://" + proxy_ip + ":" + proxy_port,
                'https': "http://" + proxy_ip + ":" + proxy_port,
            }

        except Exception as e:
            self.proxy_list = dict()
            print("changeProxy: Unexpected exception = ", e)
            pass

        self.proxy_host, self.proxy_port = proxy_ip, proxy_port

        print("changeProxy: proxy_list => ", self.proxy_list)
        self.makeRequestInitCookies()

        self.req_count = 0

        return


    # -----------------------------------------------------------------------------------------------
    def getProduct(self, url):

        global vendor_numb

        address = ""
        h1 = ""
        art_no = ""
        oem_no = ""
        prod_descr = ""
        prod_title = ""
        prod_price = ""
        photos_arr = []

        final_result = False
        n_try = 0

        while (True):
            while (True):
                resp, status_code = self.makeRequest(url)

                if resp is None:
                    global_vars.main_logger.error("HTTP-request: Empty response; request = " + url)
                    if n_try > 15:
                       return False, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                    n_try = n_try + 1
                    continue

                if status_code != 200:
                    if status_code == 403:
                        global_vars.main_logger.error("getProduct: HTTP-status code (403) = " + str(status_code) + "; request = " + url)
                        print("getProduct: HTTP-status code (403)")
                        if n_try > 3:
                            return False, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                        n_try = n_try + 1
                        self.markProxyBanned()
                        self.changeProxy()
                        time.sleep(5)
                    else:
                        global_vars.main_logger.error("getProduct: HTTP-status code = " + str(status_code) + "; request = " + url)
                        if n_try > 15:
                            return False, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                        n_try = n_try + 1
                        time.sleep(3)
                else:
                    if self.detectBan(resp.text):
                        print("detected ban ----------------------------------------------------------------------------------------------------- BAN ---------")
                        if n_try > 2:
                            return False, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                        n_try = n_try + 1
                        self.markProxyBanned()
                        self.changeProxy()
                        time.sleep(3)
                    else:
                        break

            try:
                page_doc = lxml.html.document_fromstring(resp.text)
            except AttributeError as err:
                global_vars.main_logger.error("Error getting http-response: AttributeError => " + str(err))
                page_doc = None

            if page_doc is None:
                global_vars.main_logger.error("Can not parse html-page. xml.html.document_fromstring  returns None")
                continue

            try:
                prod_title = page_doc.xpath("/html/body/div[1]/div/div/main/div[2]/div/section/header/h1")[0].xpath("text()")[0].strip()
                print("----- 1 ---> prod_title = " + prod_title)
            except:
                print("----- 1 ---> prod_title = None")
                # print("html => " + resp.text)
                prod_title = ""
            m = re.search(r'(.*)\s(\w+)$', prod_title)
            if m:
                g = m.groups()
                prod_title = g[0] + ' OEM-' + g[1]
            # print("--------> prod_title = " + prod_title)

            try:
                town = page_doc.xpath('//meta[@itemprop="addressLocality"]/@content')[0]
                town = " в г. " + town
            except: town = ""

            try:
                art_no = page_doc.xpath('//*[@id="bcur"]')[0].xpath("text()")[0].strip() + "-" + str(vendor_numb)
                print("---- 1 ----> art_no = " + str(art_no))
            except:
                art_no = ""
            # print("--------> art_no = " + str(art_no))

            prod_title = prod_title + " Под заказ (" + art_no + ")"
            print("--- fin. -----> prod_title = " + prod_title)

            try:
                oem_no = page_doc.xpath("/html/body/div[1]/div/div/main/div[2]/div/section/div[1]/div[2]/span[2]/span")[0].xpath("text()")[0].strip()
                print("----- 1 ---> oem_no = " + str(oem_no))
            except:
                oem_no = ""
                print("----- 1 ---> oem_no = None")
            # print("--------> oem_no = " + str(oem_no))

            # try: prod_descr = page_doc.xpath("/html/body/div[1]/div/div/main/div[2]/div/section/div[1]/div[2]/span[5]/span")[0].strip()
            # except: prod_descr = None
            try:
                prod_descr = page_doc.xpath("/html/body/div[1]/div/div/main/div[2]/div/section/div[1]/div[2]/span[5]/span")[0].xpath("text()")[0].strip()
                print("---- 1 ----> prod_descr = " + str(prod_descr))
            except IndexError as err:
                prod_descr = ""
                print("---- 1 ----> prod_descr = None")
                global_vars.main_logger.error("getProduct: prod_descr is empty. err = " + str(err))
            # print("--------> prod_descr = " + str(prod_descr))

            # photos = page_doc.xpath("//a[@class=fullview]/@href")
            photos_arr = page_doc.xpath('//a[@class="fullview"]/@href')
            # print("photos_arr ----------> " + repr(photos_arr))

            try: prod_price = page_doc.xpath("/html/body/div[1]/div/div/main/div[2]/div/section/div[1]/div[2]/div/form/div/span/span")[0].xpath("text()")[0].strip()
            except: prod_price = ""
            prod_price = prod_price.replace(" ", "")

            break


        if prod_descr is None:
            prod_descr = "Данная деталь находится на складе" + town + ". Просим учитывать доставку от склада поставщика."
        else:
            prod_descr = " = ".join((prod_descr, "Данная деталь находится на складе" + town + ". Просим учитывать доставку от склада поставщика."))

        photo1 = ""
        try:
            photo1 = photos_arr[0]
            photo1 = fixPhotoURL(photo1)
        except: photo1 = ""

        photo2 = ""
        try:
            photo2 = photos_arr[1]
            photo2 = fixPhotoURL(photo2)
        except: photo2 = ""

        photo3 = ""
        try:
            photo3 = photos_arr[2]
            photo3 = fixPhotoURL(photo3)
        except: photo3 = ""

        photo4 = ""
        try:
            photo4 = photos_arr[3]
            photo4 = fixPhotoURL(photo4)
        except: photo4 = ""

        photo5 = ""
        try:
            photo5 = photos_arr[4]
            photo5 = fixPhotoURL(photo5)
        except: photo5 = ""

        photo6 = ""
        try:
            photo6 = photos_arr[5]
            photo6 = fixPhotoURL(photo6)
        except: photo6 = ""

        photo7 = ""
        try:
            photo7 = photos_arr[6]
            photo7 = fixPhotoURL(photo7)
        except: photo7 = ""

        return True, art_no, prod_title, oem_no, prod_descr, prod_price, "ПОД ЗАКАЗ", photo1, photo2, photo3, photo4, photo5, photo6, photo7, url

    # ----------------------------------------------------------------------------------------------
    def lockProduct(self, _id):
        parser_sql = """ UPDATE  products  set  status_id = 2  WHERE  id = %s """

        try:
            parser_cur = self.parser_dbh.cursor()
            parser_cur.execute(parser_sql, (_id))
            parser_cur.close()
            self.parser_dbh.commit()
            del parser_cur
        except Exception as e:
            print("lockProduct: Unexpected exception = ", e)
            pass

        return

    # ----------------------------------------------------------------------------------------------
    def saveProductData(self, _id, data):
        parser_sql = """ UPDATE  products  set  status_id = 3, result_data = %s  WHERE  id = %s """

        try:
            parser_cur = self.parser_dbh.cursor()
            parser_cur.execute(parser_sql, (data, _id))
            parser_cur.close()
            del parser_cur
            self.parser_dbh.commit()
        except Exception as e:
            print("saveProductData: Unexpected exception = ", e)
            pass

        return








    # ----------------------------------------------------------------------------------------------
    def parser(self, x, z):
        print("parser: x = ", x)
        print("--> parser: z => ", z)
        self.run("F")
        time.sleep(1)

        return

    # ----------------------------------------------------------------------------------------------
    def go(self):
        p = Pool(self.num_proc, logger_worker_init, [global_vars.logger_queue])

        proc_pool = []
        for ii in range(self.num_proc):
            proc_pool.append({'func': "parser", 'v': ii, })

        res = p.map(self, proc_pool)

        p.close()
        p.join()


    # ----------------------------------------------------------------------------------------------
    def run(self, func):

        global_vars.main_logger.info("---> run (" + func + "): conn => " + repr(self.proxy_db_conn))


    # ----------------------------------------------------------------------------------------------
    def __call__(self, x):
        self.proxy_db_conn  = global_vars.proxy_db_pool._pool.get_connection()
        self.parser_db_conn = global_vars.parser_db_pool._pool.get_connection()

        if x["func"] == "parser":
            self.parser(x["v"], 'fffffff')

