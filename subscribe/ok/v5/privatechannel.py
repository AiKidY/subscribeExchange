#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import threading
import zmq
import os

from subscribe.utils.const import const
from subscribe.ok.v5.ws_base import WebsocketBase
from utils.function_util import *
from utils.datetime_util import fridays


class PrivateChannel(WebsocketBase):
    def __init__(self):
        self.initial = True
        WebsocketBase.__init__(self, auth=True)
        self.zmq_socket_dict = {}
        self.sub_url = const.WS_PIRVATE_SIMULATE_CHANNEL_OK_V5
        self.zmq_position_port = const.ZMQ_POSITION_PORT_OK_V5
        self.zmq_assets_port = const.ZMQ_ASSETS_PORT_OK_V5

    def format_position_message(self, message):
        pub_message_list = []

        if message:
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for data_dict in data:
                    mgnMode = data_dict.get('mgnMode', '')
                    if mgnMode != 'cross': continue

                    posSide = data_dict.get('posSide', '')
                    pTime = data_dict.get('pTime', '')
                    instid = data_dict.get('instId', '')
                    pos = abs(int(data_dict.get('pos', 0)))
                    avgPx = data_dict.get('avgPx', 0)
                    upl = data_dict.get('upl', 0)  # 未实现收益

                    pub_message = {'account_type': 'v5',
                                   'instid': instid,  # BTC-USDT-210625
                                   'contract_size': pos,
                                   'open_price': avgPx,
                                   'unrealized_profit': upl,
                                   'updatetime': pTime,
                                   'sendtime': int(time.time() * 1000),
                                   'direction': 1 if posSide == 'long' else -1,
                                   'account_id': account_id,
                                   'exchange': 'OK',
                                   'exchange_id': 1002
                                   }
                    pub_message_list.append(pub_message)

        return pub_message_list

    def format_assets_message(self, message):
        pub_message_list = []

        if message:
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for data_dict in data:
                    details = data_dict.get('details', [])
                    for detail in details:
                        ccy = detail.get('ccy', '')
                        eq = detail.get('eq', 0)  # 币种总权益
                        cashBal = detail.get('cashBal', 0)  # 币种余额
                        availEq = detail.get('availEq', 0)  # 可用保证金
                        frozenBal = detail.get('frozenBal', 0)  # 币种占用余额
                        uTime = detail.get('uTime', '')

                        pub_message = {'account_type': 'v5',
                                       'account_id': account_id,
                                       'asset': ccy,
                                       'total': eq,
                                       'available': availEq,
                                       'frozen': frozenBal,
                                       'updatetime': uTime,
                                       'sendtime': '',
                                       'exchange': 'OK',
                                       'exchange_id': 1002,
                                       'recvtime': int(time.time() * 1000)
                                       }
                        pub_message_list.append(pub_message)

        return pub_message_list

    def handler_position_message(self, message):  # 处理持仓数据
        pub_message_list = self.format_position_message(message)

        if pub_message_list:
            for pub_message in pub_message_list:
                # zmq/redis推送消息到客户机
                print('推送持仓消息到客户机: ', pub_message)
                self.zmq_pub(self.zmq_position_port, pub_message)

    def handler_assets_message(self, message):
        pub_message_list = self.format_assets_message(message)

        if pub_message_list:
            for pub_message in pub_message_list:
                # zmq/redis推送消息到客户机
                print('推送资产消息到客户机: ', pub_message)
                self.zmq_pub(self.zmq_assets_port, pub_message)

    def handler_callback_message(self, message):
        arg = message.get('arg', {})
        channel = arg.get('channel', '')

        if str(channel) == 'positions':  # 持仓频道
            threading.Thread(target=self.handler_position_message, args=(message,)).start()
        elif str(channel) == 'account':  # 资产频道
            threading.Thread(target=self.handler_assets_message, args=(message,)).start()

    def start(self, account_id, auth_params):  # 订阅消息
        self.initial = False
        self.init_auth_params(account_id, auth_params)
        self.main(account_id)

    def _check(self, account_params_list):
        t_account_id_list = [item.get('account_id', '') for item in account_params_list]
        account_id_list = list(self.account_dict.keys())
        return t_account_id_list == account_id_list

    def sub(self, process_name=None, sub_requests=None, sub_url=None):
        ''' 订阅 '''
        self.pid = os.getpid()
        if not sub_requests: sub_requests = self.get_sub_requests()
        self.sub_requests = sub_requests

        if sub_url: self.sub_url = sub_url

        while True:
            account_params_list = self.get_account_params()
            # account_params_list = self.get_account_params_bak()  # TODO 测试账户

            if self._check(account_params_list):
                print('无变化')
                time.sleep(60)  # 60秒监听一次表变化
                continue

            if not self.initial: self.clear_deleted_account(account_params_list)  # 针对删除的账户

            for account_params in account_params_list:
                access_key = account_params.get('access_key')
                secret_key = account_params.get('secret_key')
                pass_phrase = account_params.get('pass_phrase')
                account_id = account_params.get('account_id')

                if account_id not in self.account_dict.keys():
                    auth_params = {
                        'access_key': access_key,
                        'secret_key': secret_key,
                        'pass_phrase': pass_phrase
                    }
                    self.start(account_id, auth_params)
        return process_name, self.pid

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
            # 订阅持仓
            args = []
            for contract_date in fridays():
                arg = {
                    "channel": "positions",
                    "instType": "FUTURES",
                    "uly": "{}-USD".format(currency.upper()),
                    "instId": "{}-USD-{}".format(currency.upper(), contract_date)
                }

                args.append(arg)

            sub_request = {
                "op": "subscribe",
                "args": args
            }
            sub_requests.append(sub_request)

            # 订阅资产
            sub_request = {
                "op": "subscribe",
                "args": [{
                    "channel": "account",
                    "ccy": currency.upper()
                }]
            }
            sub_requests.append(sub_request)

        return sub_requests

    def clear_deleted_account(self, account_params_list):
        t_account_id_list = [account_params.get('account_id') for account_params in account_params_list]
        non_account_id_list = list(filter(lambda x: x not in t_account_id_list, list(self.account_dict.keys())))

        for non_account_id in non_account_id_list:
            if non_account_id in self.account_dict.keys():
                self.account_dict.pop(non_account_id)

    def get_account_params_bak(self):  # TODO 测试
        account_params = [
            {'access_key': '28ef7de2-03df-4940-a4e6-9488990233e7',
             'secret_key': 'C51DA3C59789547440AEA04D7604E691',
             'pass_phrase': 'Georgetest',
             'account_id': 'Georgetest'
             },
            {'access_key': '78dccc2a-dfcc-40e4-9c6c-a6d0651fde70',
             'secret_key': 'D685714BD7DEBED48B8DD15372123FC9',
             'pass_phrase': 'liuyongok02',
             'account_id': 'liuyongok02'
             }
        ]
        return account_params

    def get_account_params(self):  # 数据库取OK的v5统一账户
        account_params_list = get_all_data(strategy_mysql_conn, 'account_manager',
                                           ['account_id', 'access_key', 'secret_key', 'pass_phrase'],
                                           ['account_type', 'exchange', 'status', 'is_delete'],
                                           ['unified-account', 'OK', '1', '0'])

        return account_params_list

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


if __name__ == '__main__':
    pass
