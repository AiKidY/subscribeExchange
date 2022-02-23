#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import re
import zmq
import threading
import os

from subscribe.utils.const import const
from subscribe.hb.ws_base import WebsocketBase
from utils.function_util import *


class PrivateChannel(WebsocketBase):
    def __init__(self):
        self.initial = True
        WebsocketBase.__init__(self, auth=True)
        self.zmq_socket_dict = {}
        self.sub_url = const.WS_FUTURES_ORDER_URL_HB
        self.zmq_position_port = const.ZMQ_POSITION_PORT_HB
        self.zmq_assets_port = const.ZMQ_ASSETS_PORT_HB

    def format_position_message(self, message):
        pub_message_list = []

        if message:
            ts = message.get('ts', '')
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for data_dict in data:
                    currency = data_dict.get('symbol', '').upper()
                    contract_type = data_dict.get('contract_type', '')
                    contract_date = get_contract_type(contract_type)
                    instid = '{}-{}-{}'.format(currency.upper(), 'USD', contract_date)
                    volume = data_dict.get('volume', 0)
                    cost_open = data_dict.get('cost_open', 0)  # 开仓均价
                    profit_unreal = data_dict.get('profit_unreal', 0)  # 未实现盈亏
                    direction = data_dict.get('direction', '')  # buy 多仓

                    pub_message = {'instid': instid,  # BTC-USDT-210625
                                   'contract_size': volume,
                                   'open_price': cost_open,
                                   'unrealized_profit': profit_unreal,
                                   'updatetime': ts,
                                   'sendtime': '',
                                   'direction': 1 if direction == 'buy' else -1,
                                   'account_id': account_id,
                                   'exchange': 'HB',
                                   'exchange_id': 1001,
                                   'recvtime': int(time.time() * 1000)
                                   }
                    pub_message_list.append(pub_message)

        return pub_message_list

    def format_assets_message(self, message):
        pub_message_list = []

        if message:
            ts = message.get('ts', '')
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for data_dict in data:
                    currency = data_dict.get('symbol', '').upper()
                    margin_position = data_dict.get('margin_position', 0)
                    margin_balance = data_dict.get('margin_balance', 0)  # 账户权益
                    margin_frozen = data_dict.get('margin_frozen', 0)
                    margin_available = data_dict.get('margin_available', 0)
                    profit_unreal = data_dict.get('profit_unreal', 0)

                    pub_message = {'account_id': account_id,
                                   'exchange': 'HB',
                                   'asset': currency,
                                   'total': margin_balance,
                                   'available': margin_available,
                                   'frozen': margin_frozen,
                                   'margin_position': margin_position,
                                   'unrealized_profit': profit_unreal,
                                   'updatetime': ts,
                                   'sendtime': '',
                                   'exchange_id': 1001,
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
                # self.zmq_pub(config.zmq_positions_port., pub_message)  # config.zmq_positions_port

    def handler_assets_message(self, message):  # 处理资产数据
        pub_message_list = self.format_assets_message(message)

        if pub_message_list:
            for pub_message in pub_message_list:
                # zmq/redis推送消息到客户机
                print('推送资产消息到客户机: ', pub_message)
                self.zmq_pub(self.zmq_assets_port, pub_message)
                # self.zmq_pub(config.zmq_assets_port, message)  # config.zmq_assets_port

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)
        self.origin_account_params_list = []
        self.clear(ws)

    def handler_callback_message(self, message):
        topic = message.get('topic', '')
        print('handler_callback_message,', message, topic)

        if re.search('positions', str(topic)):  # 持仓频道
            threading.Thread(target=self.handler_position_message, args=(message,)).start()
        elif re.search('accounts', str(topic)):  # 账户频道
            threading.Thread(target=self.handler_assets_message, args=(message,)).start()

    def start(self, account_id, auth_params):  # 订阅消息
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

    def clear_deleted_account(self, account_params_list):
        t_account_id_list = [account_params.get('account_id') for account_params in account_params_list]
        non_account_id_list = list(filter(lambda x: x not in t_account_id_list, list(self.account_dict.keys())))

        for non_account_id in non_account_id_list:
            if non_account_id in self.account_dict.keys():
                self.account_dict.pop(non_account_id)

    def get_account_params_bak(self):  # TODO 测试
        account_params = [
            {'access_key': '68f4ec81-vftwcr5tnh-74735fd2-ebe6a',
             'secret_key': '3ae9e689-4e89a690-f15d90f2-5f119',
             'pass_phrase': 'sc24680',
             'account_id': 'sc24680'
             }
        ]
        return account_params

    def get_account_params(self):
        account_params_list = get_all_data(strategy_mysql_conn, 'account_manager',
                                           ['account_id', 'access_key', 'secret_key', 'pass_phrase'],
                                           ['exchange', 'status', 'is_delete'], ['HB', '1', '0'])

        return account_params_list

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
            if currency.upper() == 'USD':
                continue

            # 订阅持仓
            sub_position_request = {
                "op": "sub",
                "cid": str(uuid.uuid1()),
                "topic": "positions.{}".format(currency.lower())
            }

            # 订阅资产
            sub_assets_request = {
                "op": "sub",
                "cid": str(uuid.uuid1()),
                "topic": "accounts.{}".format(currency.lower())
            }

            sub_requests.extend([sub_position_request, sub_assets_request])

        return sub_requests

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
