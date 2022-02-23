#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import threading

import asyncio, traceback, nest_asyncio
from subscribe.utils.const import const
from strategy_order.ws_socket_server.zmq_pub_sub import ZmqMain
from strategy_order.ws_socket_server.websocket_base import PublicSocketCls
from utils.function_util import *

nest_asyncio.apply()


class AssetsPush(PublicSocketCls, ZmqMain):
    def __init__(self):
        PublicSocketCls.__init__(self)
        ZmqMain.__init__(self)
        self.ws_port = const.WS_ASSERTS_PORT
        self.new_event_loop = asyncio.new_event_loop()

    def get_tasks(self):
        tasks = {
            'assets_hb': {'port': const.ZMQ_ASSETS_PORT_HB, 'timeout': const.ZMQ_ASSETS_TIMEOUT},
            'assets_ok_v3': {'port': const.ZMQ_ASSETS_PORT_OK_V3, 'timeout': const.ZMQ_ASSETS_TIMEOUT},
            'assets_ok_v5': {'port': const.ZMQ_ASSETS_PORT_OK_V5, 'timeout': const.ZMQ_ASSETS_TIMEOUT},
        }
        return tasks

    def get_cur_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    async def zmq_callback_func(self, data, client=None):
        data['sendtime'] = self.get_cur_time()
        self.new_event_loop.run_until_complete(asyncio.wait([self.socket_send_message(data), ]))
        print('client: {}, 推送前端消息成功: {}'.format(client, data))

    def start(self):
        for client, value in self.get_tasks().items():
            port = value.get('port', '')
            timeout = value.get('timeout', '')
            print('----- 启动线程, client: {}, port: {} -----'.format(client, port))
            threading.Thread(target=self.zmq_main, args=(port, timeout, self.zmq_callback_func, client,)).start()
        self.ws_main()


if __name__ == '__main__':
    AssetsPush().start()
