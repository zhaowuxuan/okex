#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# encoding: utf-8
# 
# Market Real-time Subscription v3
#
# Copyright 2019 FawkesPan
#
# Do What the Fuck You Want To Public License
#


import time
import ssl
import sys
import code
import json
import hashlib
import hmac
import urllib
import threading
from distutils.log import Log

import websocket
import zlib
import string

import get_open_browser_chrome

try:
    import readline
except ImportError:
    pass

pong = time.time()


# def operator(op, args, ws):
#     message = {
#         'op': op,
#         'args': args
#     }
#     ws.send(json.dumps(message))


class WSSubscription:

    def __init__(self, instrument_id='PEOPLE-USD-SWAP', channel='open-interest', on_message=None):
        self.__iid = instrument_id
        self.__channel = channel
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
            # operator('subscribe', [{"channel": self.__channel, "instType": self.__iid}])
            operator('subscribe', [{"channel": "open-interest", "instId": "PEOPLE-USDT-SWAP"}])

            while True:
                ws.send("ping")
                time.sleep(30)

        threading.Thread(target=run).start()

    def sub(self):

        websocket.enableTrace(False)
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
        print(message)
        global pong
        if 'pong' in message:
            pong = time.time()
        if 'asks' in message and 'bids' in message:
            d = json.loads(message)
            self.__Depth = d['data'][0]

        if self.__callbackEnabled:
            self.__callback(message)

    def error_handling(self, ws, error):
        print(str(error))

    def closing(self, ws):
        print("WebSocket Closing...")
        # operator('unsubscribe', [{"channel": "open-interest", "instId": "PEOPLE-USDT-SWAP"}],ws)


OkEXWS = WSSubscription


# 模块测试
def main():
    OkEX = OkEXWS('PEOPLE-USDT-SWAP', 'open-interest')
    # cols = ["channel", "inst_id", "inst_type", "oi", "oi_ccy", "ts"]
    while True:
        result = OkEX.GetDepth()
        print(list(result))
        # if result["data"]:
        #     col_data = [result["arg"]["channel"], result["data"][0]["instId"], result["data"][0]["instType"]
        #         , result["data"][0]["oi"], result["data"][0]["oiCcy"], result["data"][0]["ts"]]
        #     get_open_browser_chrome.insert_table("cc_swap_open_interest", cols, col_data)
        time.sleep(3)


if __name__ == '__main__':
    main()
