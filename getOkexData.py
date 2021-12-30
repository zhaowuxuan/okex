#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# encoding: utf-8
import logging
import time
import ssl
import sys
import code
import json
import hashlib
import hmac
import urllib
import threading

import sqlalchemy
import websocket
import zlib
import string

from sqlalchemy import create_engine, text

from MyTest.snow import get_open_browser_chrome

try:
    import readline
except ImportError:
    pass

pong = time.time()


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


# 模块测试
def load_data(message):
    cols = ["channel", "inst_id", "inst_type", "oi", "oi_ccy", "ts"]
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


class WSSubscription:

    def __init__(self, instrument_id='BTC-USD-190517', market='futures', on_message=None):
        self.__iid = instrument_id
        self.__market = market
        self.__Depth = {}

        if on_message is not None:
            self.__callbackEnabled = True
            self.__callback = on_message
        else:
            self.__callbackEnabled = False

        thread = threading.Thread(target=self.sub, args=())
        thread.daemon = True
        thread.start()

    def GetDepth(self):
        return self.__Depth

    def subscribe(self, ws):

        def operator(op, args):
            message = {
                'op': op,
                'args': args
            }
            ws.send(json.dumps(message))

        def run(*args):
            # operator('subscribe', ['%s/depth5:%s' % (self.__market, self.__iid)])
            # operator('subscribe', ['%s/trade:%s' % (self.__market, self.__iid)])
            operator('subscribe', [{"channel": "open-interest", "instId": "PEOPLE-USDT-SWAP"}])
            operator('subscribe', [{"channel": "open-interest", "instId": "BTC-USDT-SWAP"}])
            operator('subscribe', [{"channel": "open-interest", "instId": "ETH-USDT-SWAP"}])
            operator('subscribe', [{"channel": "open-interest", "instId": "SAND-USDT-SWAP"}])

            while True:
                ws.send("ping")
                time.sleep(30)

        threading.Thread(target=run).start()

    def sub(self):

        websocket.enableTrace(False)
        # URL = "wss://wsaws.okex.com:8443/ws/v5/public"
        URL = "wss://ws.okex.com:8443/ws/v5/public"
        ws = websocket.WebSocketApp(URL,
                                    on_message=self.incoming,
                                    on_error=self.error_handling,
                                    on_close=self.closing)

        ws.on_open = self.subscribe
        while True:
            try:
                ws.run_forever()
            except:
                pass

        pass

    def incoming(self, ws, message):
        # message = zlib.decompress(message, -zlib.MAX_WBITS)
        # message = message.decode('utf-8')
        # print(0)
        print(message)
        global pong
        if 'pong' in message:
            pong = time.time()
        if 'arg' in message and 'data' in message:
            d = json.loads(message)
            self.__Depth = message
            load_data(d)
        if self.__callbackEnabled:
            self.__callback(message)

    def error_handling(self, ws, error):
        print(str(error))

    def closing(self, ws):
        print("WebSocket Closing...")


OkEXWS = WSSubscription


if __name__ == '__main__':
    print("beign!")
    OkEX = OkEXWS('BTC-USD-190517', 'futures')
