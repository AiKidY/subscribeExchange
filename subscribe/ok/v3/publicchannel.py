#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import zmq
import json
import threading

from utils.datetime_util import fridays
from subscribe.utils.const import const
from subscribe.ok.v3.ws_base import WebsocketBase
from utils.function_util import *


class PublicChannel(WebsocketBase):
    def __init__(self):
        self.initial = True
        WebsocketBase.__init__(self, auth=False)
        self.zmq_socket_dict = {}
        self.sub_url = const.WS_URL_OK_V3
        self.origin_sub_requests = []
        self.zmq_quotation_port = const.ZMQ_QUOTATION_PORT_OK_V3

    def format_quotation_message(self, message):  # 处理行情数据格式、
        pub_message_list = []

        if message:
            data = message.get('data', [])
            if data:
                for data_dict in data:
                    instid = data_dict.get('instrument_id', '')
                    timestamp = data_dict.get('timestamp', '')
                    last = data_dict.get('last', '')
                    last_qty = data_dict.get('last_qty', '')
                    if timestamp:
                        timestamp_format = timestamp.split('.')[0]
                        timestamp = timestamp_format[: 10] + ' ' + timestamp_format[11:]
                        timestamp = date_to_timestamp(timestamp, 3)

                    pub_message = {'account_type': 'v3',
                                   'data_type': 'trade',
                                   'instid': instid,  # 产品id, 币种 + 保证金 + 合约类型
                                   'updatetime': timestamp,
                                   'volume': last_qty,
                                   'price': last,
                                   'exchange': 'OK',
                                   'exchange_id': 1002,
                                   'recvtime': int(time.time() * 1000),
                                   'sendtime': ''
                                   }
                    pub_message_list.append(pub_message)

        return pub_message_list

    def handler_quotation_message(self, message):  # 处理行情数据
        print('handler_quotation_message', type(message), message)
        pub_message_list = self.format_quotation_message(message)

        if pub_message_list:
            for pub_message in pub_message_list:
                # zmq/redis推送消息到客户机
                print('推送消息到客户机: ', pub_message)
                self.zmq_pub(self.zmq_quotation_port, pub_message)

    def handler_callback_message(self, message):
        table = message.get('table', '')

        if re.search('ticker', str(table)):  # 行情频道(合约,现货)
            threading.Thread(target=self.handler_quotation_message, args=(message,)).start()

    def get_request_args(self, sub_request):
        args = sub_request.get('args', [])
        return args

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)
        self.clear(ws)
        self.initial = True
        self.origin_sub_requests = []

    def _check(self, sub_requests):  # 增量订阅请求
        self.sub_requests = list(filter(lambda _: _ not in self.sub_requests, sub_requests))  # 更新订阅请求
        _sub_requests = list(filter(lambda _: _ not in self.sub_requests, self.origin_sub_requests))  # 更新取消订阅请求

        # 无需再订阅(币种对应)
        for sub_request in _sub_requests:
            op = sub_request.get('op', '')
            args = sub_request.get('args', [])

            if op == 'subscribe':
                non_sub_request = {
                    "op": "unsubscribe",
                    "args": args
                }
                self.non_sub_requests.append(non_sub_request)

    def sub(self, process_name=None, _sub_requests=None, sub_url=None):
        self.pid = os.getpid()
        if sub_url: self.sub_url = sub_url
        sub_requests = _sub_requests

        while True:
            if not _sub_requests: sub_requests = self.get_sub_requests()  # 默认订阅公有频道行情数据(都是需要订阅的) 实时更新

            if self.origin_sub_requests == sub_requests:
                time.sleep(60)
                continue

            self._check(sub_requests)
            self.origin_sub_requests = self.sub_requests

            if self.initial: self.main()
            self.initial = False

        return process_name, self.pid

    def zmq_pub(self, zmq_port, message):
        ''' zmq发布消息 '''
        if str(zmq_port) not in self.zmq_socket_dict.keys():
            context = zmq.Context()
            socket = context.socket(zmq.PUB)
            socket.bind("tcp://*:{}".format(zmq_port))
            self.zmq_socket_dict[str(zmq_port)] = socket  # 注册zmq_socket
        else:
            socket = self.zmq_socket_dict.get(str(zmq_port))

        socket.send_string(json.dumps(message))
        print('zmq发布消息成功', message)

    def get_currency_list(self):
        cursor, conn, currency_list = None, None, list()
        try:
            search_sql = 'select show_text from sys_dict_data where type_id = (select id from sys_dict_type where field_code = "currency")'

            connection = strategy_mysql_conn()
            conn, cursor = connection.get('conn'), connection.get('cursor')

            cursor.execute(search_sql)
            datas = cursor.fetchall()

            if datas:
                for data in datas:
                    currency_list.append(data[0])
        except Exception as e:
            print(e)
        finally:
            return currency_list

    def get_sub_requests(self):
        currency_list = self.get_currency_list()

        sub_requests = []
        for currency in currency_list:

            # 订阅合约行情
            instId_list = []
            for contract_date in fridays():
                instId_list.append('{}-{}-{}'.format(currency.upper(), 'USD', contract_date))

            args = []
            for instId in instId_list:
                # {"op": "subscribe", "args": ["futures/ticker:BTC-USD-170310"]}
                args.append('futures/ticker:{}'.format(instId))

            sub_request = {
                "op": "subscribe",
                "args": args
            }
            sub_requests.append(sub_request)

            # 订阅现货行情
            sub_request = {
                "op": "subscribe",
                "args": ["spot/ticker:{}-USDT".format(currency.upper())]
            }
            sub_requests.append(sub_request)
            # {"op": "subscribe", "args": ["spot/ticker:ETH-USDT"]}

        return sub_requests


if __name__ == '__main__':
    obj = PublicChannel()
    sub_url = 'wss://ws.ok.com:8443/ws/v5/public'
    sub_requests = [{
        "op": "subscribe",
        "args": [{
            "channel": "tickers",
            "instId": "BTC-USD-210604"
        }]
    }]

    obj.sub(sub_url, sub_requests)
