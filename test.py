import json
import urllib

import sqlalchemy
from sqlalchemy import *

import get_open_browser_chrome
import logging

message = {"arg": {"channel": "open-interest", "instId": "PEOPLE-USDT-SWAP"}, "data":
    [{"instId": "PEOPLE-USDT-SWAP", "instType": "SWAP", "oi": "1741921", "oiCcy": "174192100", "ts": "1640757489143"}]}
cols = ["channel", "inst_id", "inst_type", "oi", "oi_ccy", "ts"]
# 驱动初始化
ENGINE = create_engine('mysql+pymysql://mall_tiny:%s@localhost:3306/mall_ums' % urllib.parse.quote('1233rs!@d'),
                       echo=False)


def insert_table(p_table_name, p_cols, p_cols_data):
    data = list(map(get_open_browser_chrome.turn_to_mysql_data, p_cols_data, p_cols))
    logging.debug('insert_table cols= ' + '(' + ','.join(p_cols) + ')')
    logging.debug('insert_table values= ' + '(' + ','.join(data) + ')')
    sql = "insert into {0} {1} values {2};".format(p_table_name, '(' + ','.join(p_cols) + ')',
                                                   '(' + ','.join(data) + ')')
    with ENGINE.connect() as connection:
        result = connection.execute(text(sql))
        logging.debug("insert_table sql=" + sql)
    return result


if message["data"]:
    col_data = [message["arg"]["channel"], message["data"][0]["instId"], message["data"][0]["instType"]
        , message["data"][0]["oi"], message["data"][0]["oiCcy"], message["data"][0]["ts"]]
    try:
        insert_table("cc_swap_open_interest", cols, col_data)
    except sqlalchemy.exc.IntegrityError as e:
        # print(e._message)
        # print(str(e))
        # print(repr(e))
        if e.orig.args[0] == 1062:
            pass
        else:
            raise e
    else:
        print("success")

# if 'arg' in message and 'data' in message:
#     # d = json.loads(message)
#     print([message["arg"]["channel"], message["data"][0]["instId"], message["data"][0]["instType"]
#         , message["data"][0]["oi"], message["data"][0]["oiCcy"], message["data"][0]["ts"]])
#     print(message)
