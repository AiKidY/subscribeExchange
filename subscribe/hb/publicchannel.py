#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import zmq
import threading

from subscribe.hb.ws_base import WebsocketBase
from subscribe.utils.const import const
from utils.datetime_util import fridays
from utils.function_util import *


class PublicChannel(WebsocketBase):
    def __init__(self):
        self.initial = True
        WebsocketBase.__init__(self, auth=False)
        self.zmq_socket_dict = {}
        self.sub_futures_url = const.WS_FUTURES_QUOTATION_URL_HB
        self.sub_spot_url = const.WS_SPOT_QUOTATION_URL_HB
        self.origin_sub_requests = []
        self.zmq_futures_quotation_port = const.ZMQ_FUTURES_QUOTATION_PORT_HB
        self.zmq_spot_quotation_port = const.ZMQ_SPOT_QUOTATION_PORT_HB

    def format_quotation_message(self, message):  # 处理行情数据格式
        if message:
            tick = message.get('tick', {})
            if not tick: return

            ch = message.get('ch', '')
            ch_str = ch.split('.')[1].lower()
            if re.search('usdt', ch_str):
                currency = ch_str.replace('usdt', '')
                deposit_type = 'usdt'
                instid = '{}-{}'.format(currency.upper(), deposit_type.upper())
            else:
                contract_date = ch_str[-6:]
                currency = ch_str.replace(contract_date, '')
                deposit_type = 'usd'
                instid = '{}-{}-{}'.format(currency.upper(), deposit_type.upper(), contract_date)

            pub_message = {'data_type': 'trade',
                           'instid': instid,  # 产品id, 币种 + 保证金 + 合约类型
                           'updatetime': message.get('ts', ''),
                           'volume': tick.get('amount', 0),
                           'price': tick.get('close', 0),
                           'exchange': 'HB',
                           'exchange_id': 1001,
                           'recvtime': int(time.time() * 1000),
                           'sendtime': ''
                           }
            return pub_message

    def handler_quotation_message(self, message, zmq_quotation_port):  # 处理行情数据
        print('handler_quotation_message', type(message), message)

        # 处理格式
        message = self.format_quotation_message(message)
        if message:
            # zmq/redis推送消息到客户机
            print('推送消息到客户机: ', message)
            self.zmq_pub(zmq_quotation_port, message)

    def handler_callback_message(self, message):
        ch = message.get('ch', '')

        if re.search('market.*usdt.detail', str(ch).lower()):  # 现货行情
            threading.Thread(target=self.handler_quotation_message,
                             args=(message, self.zmq_spot_quotation_port,)).start()
        elif re.search('market.*.detail', str(ch).lower()):  # 合约行情
            threading.Thread(target=self.handler_quotation_message,
                             args=(message, self.zmq_futures_quotation_port,)).start()

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)
        self.clear(ws)
        self.initial = True
        self.origin_sub_requests = []

    def sub_futures(self, process_name=None, _sub_requests=None, sub_url=None):
        self.pid = os.getpid()
        self.sub_url = sub_url if sub_url else self.sub_futures_url
        sub_requests = _sub_requests

        while True:
            if not _sub_requests: sub_requests = self.get_sub_futures_requests()  # 默认订阅公有频道行情数据(都是需要订阅的) 实时获取

            if self.origin_sub_requests == sub_requests:  # 无变化, 维持原服务
                print('合约无变化')
                time.sleep(60)
                continue

            self._check(sub_requests)
            self.origin_sub_requests = self.sub_requests

            if self.initial: self.main()
            self.initial = False

        return process_name, self.pid

    def _check(self, sub_requests):  # 增量订阅请求
        self.sub_requests = list(filter(lambda _: _ not in self.sub_requests, sub_requests))  # 更新订阅请求
        _sub_requests = list(filter(lambda _: _ not in self.sub_requests, self.origin_sub_requests))  # 更新取消订阅请求

        # 无需再订阅(币种对应)
        for sub_request in _sub_requests:
            if 'sub' in sub_request.keys():
                id = sub_request.get('id', '')
                topic = sub_request.get('sub', '')

                non_sub_request = {
                    "unsub": topic,
                    "id": id
                }
                self.non_sub_requests.append(non_sub_request)

    def sub_spot(self, process_name=None, _sub_requests=None, sub_url=None):
        self.pid = os.getpid()
        self.sub_url = sub_url if sub_url else self.sub_spot_url
        sub_requests = _sub_requests

        while True:
            if not _sub_requests: sub_requests = self.get_sub_spot_requests()  # 默认订阅公有频道行情数据(都是需要订阅的) 实时获取

            if self.origin_sub_requests == sub_requests:  # 无变化, 维持原服务
                print('现货无变化')
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

    def get_sub_futures_requests(self):
        currency_list = self.get_currency_list()

        sub_requests = []
        for currency in currency_list:
            # 订阅合约行情数据
            for contract_date in fridays():
                symbol = '{}{}'.format(currency.upper(), contract_date)

                sub_request = {
                    "sub": "market.{}.detail".format(symbol),
                    "id": ''
                }
                sub_requests.append(sub_request)

        return sub_requests

    def get_sub_spot_requests(self):
        currency_list = self.get_currency_list()

        sub_requests = []
        for currency in currency_list:
            # 订阅现货行情数据
            symbol = '{}{}'.format(currency.lower(), 'usdt')

            sub_request = {
                "sub": "market.{}.detail".format(symbol),
                "id": ""
            }
            sub_requests.append(sub_request)

        return sub_requests


if __name__ == '__main__':
    pass
