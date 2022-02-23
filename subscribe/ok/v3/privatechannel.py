#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import re
import threading
import zmq
import os

from subscribe.utils.const import const
from subscribe.ok.v3.ws_base import WebsocketBase
from utils.function_util import *
from utils.datetime_util import fridays


class PrivateChannel(WebsocketBase):
    def __init__(self):
        self.initial = True
        WebsocketBase.__init__(self, auth=True)
        self.zmq_socket_dict = {}
        self.sub_url = const.WS_URL_OK_V3
        self.zmq_position_port = const.ZMQ_POSITION_PORT_OK_V3
        self.zmq_assets_port = const.ZMQ_ASSETS_PORT_OK_V3

    def format_position_message(self, message):
        pub_message_list = []

        if message:
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for data_dict in data:
                    margin_mode = data_dict.get('margin_mode', '')
                    if margin_mode != 'crossed': continue  # 全仓

                    instid = data_dict.get('instrument_id', '')
                    long_qty = abs(int(data_dict.get('long_qty', 0)))  # 多仓数量
                    short_qty = abs(int(data_dict.get('short_qty', 0)))  # 空仓数量
                    long_avg_cost = data_dict.get('short_avg_cost', 0)  # 开仓平均价(多头)
                    short_avg_cost = data_dict.get('short_avg_cost', 0)  # 开仓平均价(空头)
                    updated_at = data_dict.get('updated_at', '')
                    long_unrealised_pnl = data_dict.get('long_unrealised_pnl', 0)  # 多仓未实现盈亏
                    short_unrealised_pnl = data_dict.get('short_unrealised_pnl', 0)  # 空仓未实现盈亏
                    if updated_at:
                        updated_at_format = updated_at.split('.')[0]
                        updated_at = updated_at_format[: 10] + ' ' + updated_at_format[11:]
                        updated_at = date_to_timestamp(updated_at, 3)

                    # 多头持仓
                    pub_message = {'account_type': 'v3',
                                   'pair': instid,  # BTC-USDT-210625
                                   'contract_size': long_qty,
                                   'open_price': long_avg_cost,
                                   'unrealized_profit': long_unrealised_pnl,
                                   'updatetime': updated_at,
                                   'sendtime': '',
                                   'direction': 1,
                                   'account_id': account_id,
                                   'exchange': 'OK',
                                   'exchange_id': 1002,
                                   'recvtime': int(time.time() * 1000)
                                   }
                    pub_message_list.append(pub_message)

                    # 空头持仓
                    pub_message = {'account_type': 'v3',
                                   'instid': instid,  # BTC-USDT-210625
                                   'contract_size': short_qty,
                                   'open_price': short_avg_cost,
                                   'unrealized_profit': short_unrealised_pnl,
                                   'updatetime': updated_at,
                                   'sendtime': '',
                                   'direction': -1,
                                   'account_id': account_id,
                                   'exchange': 'OK',
                                   'exchange_id': 1002,
                                   'recvtime': int(time.time() * 1000)
                                   }
                    pub_message_list.append(pub_message)

        return pub_message_list

    def format_assets_message(self, message):
        pub_message_list = []

        if message:
            account_id = message.get('account_id', '')
            data = message.get('data', [])

            if data:
                for _data in data:
                    for currency, data_dict in _data.items():
                        margin_mode = data_dict.get('margin_mode', '')
                        if margin_mode != 'crossed': continue  # 全仓

                        equity = data_dict.get('equity', 0)  # 账户权益
                        margin = data_dict.get('margin', 0)  # 已用保证金
                        available = data_dict.get('available', 0)  # 可用保证金
                        timestamp = data_dict.get('timestamp', '')
                        if timestamp:
                            timestamp_format = timestamp.split('.')[0]
                            timestamp = timestamp_format[: 10] + ' ' + timestamp_format[11:]
                            timestamp = date_to_timestamp(timestamp, 3)

                        pub_message = {'account_type': 'v3',
                                       'account_id': account_id,
                                       'asset': currency,
                                       'total': equity,
                                       'available': available,
                                       'frozen': margin,
                                       'updatetime': timestamp,
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
                # self.zmq_pub(config.zmq_positions_port., pub_message)  # config.zmq_positions_port

    def handler_assets_message(self, message):  # 处理资产数据
        pub_message_list = self.format_assets_message(message)

        if pub_message_list:
            for pub_message in pub_message_list:
                # zmq/redis推送消息到客户机
                print('推送资产消息到客户机: ', pub_message)
                self.zmq_pub(self.zmq_assets_port, pub_message)
                # self.zmq_pub(config.zmq_assets_port., pub_message)  # config.zmq_positions_port

    def handler_callback_message(self, message):
        table = message.get('table', '')

        if re.search('futures/position', str(table)):  # 持仓频道
            threading.Thread(target=self.handler_position_message, args=(message,)).start()
        elif re.search('futures/account', str(table)):  # 账户频道
            threading.Thread(target=self.handler_assets_message, args=(message,)).start()

    def start(self, account_id, auth_params):  # 订阅消息
        self.initial = True
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
            {'access_key': 'd2a0d9ad-3b28-4ae6-8a8f-c3038bbd065b',
             'secret_key': '92EEC33FB7A575FD4E84501834579422',
             'pass_phrase': 'sc24680',
             'account_id': 'sc24680'
             }
        ]
        return account_params

    def get_account_params(self):  # 数据库取OK的v3经典账户
        account_params_list = get_all_data(strategy_mysql_conn, 'account_manager',
                                           ['account_id', 'access_key', 'secret_key', 'pass_phrase'],
                                           ['exchange', 'status', 'is_delete'],
                                           ['OK', '1', '0'], ['unified-account'])

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

            instId_list = []
            for contract_date in fridays():
                instId_list.append('{}-{}-{}'.format(currency.upper(), 'USD', contract_date))

            # 订阅持仓
            args = []
            for instId in instId_list:
                args.append('futures/position:{}'.format(instId))

            sub_request = {
                "op": "subscribe",
                "args": args  # futures/position:BTC-USD-170317 持仓频道
            }
            sub_requests.append(sub_request)

            # 订阅资产
            sub_request = {
                "op": "subscribe",
                "args": ['futures/account:{}'.format(currency.upper())]  # futures/position:BTC 资产频道
            }
            sub_requests.append(sub_request)

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
