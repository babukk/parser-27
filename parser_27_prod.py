#! /usr/bin/env python
#---------------------------------------------------------------------------------------------------

import sys
import signal
import requests
import lxml.html
import time
import re
from lxml.html.soupparser import fromstring
from lxml import etree
import pymysql as PyMySQL
# import mysql.connector.pooling as mysqlPolling
import logging
from multiprocessing import Pool, Queue, cpu_count
from logging.handlers import QueueHandler, QueueListener
# import mysql.connector.pooling
import configparser
import getopt


import global_vars
 
from db_pool import DbPool
from crawler import Crawler
from logger import logger_worker_init, logger_init


# --------------------------------------------------------------------------------------------------
def getConfig(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config


# --------------------------------------------------------------------------------------------------
def get_setting(path, section, setting):
    config = getConfig(path)
    value = config.get(section, setting)

    return value


# --------------------------------------------------------------------------------------------------
def main(**kwargs):
    # global logger_queue

    logger_fname = kwargs.get('logger_fname')
    q_log_listener, global_vars.logger_queue = logger_init(logger_fname)

    num_proc = kwargs.get('num_proc')
    print("num_proc = ", num_proc)

    main_host = kwargs.get('main_host')
    print("main_host = ", main_host)


    global_vars.main_logger = logging

    crawler = Crawler(num_proc=num_proc, main_host=main_host,)

    crawler.go()

    q_log_listener.stop()


# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    _config_file = "parser.conf"
    _config_section = "parser"
    _logger_fname = None
    _num_proc = cpu_count()

    _main_host = None

    _proxy_db_config = {
        'db_host': 'localhost',
        'db_port': 3306,
        'db_user': 'root',
        'db_password': '',
        'db_database': '',
        'db_pool_size': 1,
        'db_pool_name': 'proxy_db',
    }

    _parser_db_config = {
        'db_host': 'localhost',
        'db_port': 3306,
        'db_user': 'root',
        'db_password': '',
        'db_database': '',
        'db_pool_name': 'parser_db',
        'db_pool_size': 1,
    }


    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["help", "config=",])
    except:
        # assert False, "Unhandled option."
        print("Undefined option(s).\nUsage: python " + sys.argv[0] + " [-c | --config=]config_file ")
        sys.exit(1)


    for oo, a in opts:
        if oo in ("-h", "--help"):
            print("usage: python " + sys.argv[0] + " [-c | --config=]config_file ")
            sys.exit( 0 )
        elif oo in ("-c", "--config"):
            _config_file = a
        else:
            # assert False, "Unhandled option."
            print("Undefined option(s).\nUsage: python " + sys.argv[0] + " [-c | --config=]config_file ")

    try: _logger_fname = get_setting(_config_file, _config_section, "logger_file")
    except configparser.NoSectionError as e:  print("section not found: ", str(e))
    except configparser.NoOptionError as e:  print("option not found: ", str(e))
    finally:  pass

    try: _num_proc = get_setting(_config_file, _config_section, "num_proc")
    except configparser.NoSectionError as e: print("section not found: ", str(e))
    except configparser.NoOptionError as e:   pass
    finally:  pass


    try: _main_host = get_setting(_config_file, _config_section, "main_host")
    except Exception as e: print(e)


    try: _proxy_db_config['db_host'] = get_setting(_config_file, _config_section, "proxy_db_host")
    except Exception as e: print(e)

    try: _proxy_db_config['db_port'] = get_setting(_config_file, _config_section, "proxy_db_port")
    except Exception as e: print(e)

    try: _proxy_db_config['db_user'] = get_setting(_config_file, _config_section, "proxy_db_user")
    except Exception as e: print(e)

    try: _proxy_db_config['db_password'] = get_setting(_config_file, _config_section, "proxy_db_pass")
    except Exception as e: print(e)

    try: _proxy_db_config['db_database'] = get_setting(_config_file, _config_section, "proxy_db_name")
    except Exception as e: print(e)

    try: _proxy_db_config['db_pool_name'] = get_setting(_config_file, _config_section, "proxy_db_poolname")
    except Exception as e: print(e)

    try: _proxy_db_config['db_pool_size'] = get_setting(_config_file, _config_section, "proxy_db_poolsize")
    except Exception as e: print(e)

    try: _parser_db_config['db_host'] = get_setting(_config_file, _config_section, "parser_db_host")
    except Exception as e: print(e)

    try: _parser_db_config['db_port'] = get_setting(_config_file, _config_section, "parser_db_port")
    except Exception as e: print(e)

    try: _parser_db_config['db_user'] = get_setting(_config_file, _config_section, "parser_db_user")
    except Exception as e: print(e)

    try: _parser_db_config['db_password'] = get_setting(_config_file, _config_section, "parser_db_pass")
    except Exception as e: print(e)

    try: _parser_db_config['db_database'] = get_setting(_config_file, _config_section, "parser_db_name")
    except Exception as e: print(e)

    try: _parser_db_config['db_pool_name'] = get_setting(_config_file, _config_section, "parser_db_poolname")
    except Exception as e: print(e)

    try: _parser_db_config['db_pool_size'] = get_setting(_config_file, _config_section, "parser_db_poolsize")
    except Exception as e: print(e)


    if _num_proc > _parser_db_config['db_pool_size']:
        _parser_db_config['db_pool_size'] = _num_proc

    if _num_proc > _parser_db_config['db_pool_size']:
        _proxy_db_config['db_pool_size'] = _num_proc


    global_vars.proxy_db_pool = DbPool(**_proxy_db_config)
    global_vars.parser_db_pool = DbPool(**_parser_db_config)

    main(logger_fname=_logger_fname, num_proc=_num_proc, main_host=_main_host,)
