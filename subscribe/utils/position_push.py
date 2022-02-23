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


class PositionPush(PublicSocketCls, ZmqMain):
    def __init__(self):
        PublicSocketCls.__init__(self)
        ZmqMain.__init__(self)
        self.ws_port = const.WS_POSITION_PORT
        self.new_event_loop = asyncio.new_event_loop()

    def get_tasks(self):
        tasks = {
            'position_hb': {'port': const.ZMQ_POSITION_PORT_HB, 'timeout': const.ZMQ_POSITION_TIMEOUT},
            'position_ok_v3': {'port': const.ZMQ_POSITION_PORT_OK_V3, 'timeout': const.ZMQ_POSITION_TIMEOUT},
            'position_ok_v5': {'port': const.ZMQ_POSITION_PORT_OK_V5, 'timeout': const.ZMQ_POSITION_TIMEOUT},
        }
        return tasks

    def get_cur_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def get_grid_trade_params(self, data: dict):  # 获取网格策略参数
        strategy_id, instance_id = '', ''

        if data:
            account_id = data.get('account_id', '')
            exchange_id = data.get('exchange_id', '')
            side = data.get('direction', '')
            instId = data.get('pair', '')
            instId_list = instId.split('-')
            currency = instId_list[0]
            deposit_type = instId_list[1].upper()
            contract_date = instId_list[2]
            contract_type_dict = okex_contract_type()
            contract_type = contract_type_dict.get(str(contract_date), '')

            search_dict = get_table_data(strategy_mysql_conn, const.GRID_EXCHANGES_ACCOUNT,
                                         ['strategy_id', 'instance_id'],
                                         ['account_id', 'currency', 'deposit_type', 'contract_type', 'side',
                                          'exchange_id'],
                                         [account_id, currency, deposit_type, contract_type, side, exchange_id])

            if search_dict:
                strategy_id = search_dict.get('strategy_id', '')
                instance_id = search_dict.get('instance_id', '')

        return strategy_id, instance_id

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
    PositionPush().start()
