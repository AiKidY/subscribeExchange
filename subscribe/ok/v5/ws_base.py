#!/usr/bin/env python
import websocket
import json
import datetime
import hmac
import base64
import threading
import time


class WebsocketBase:
    def __init__(self, auth):
        self.sub_url = ''
        self.sub_requests = []
        self.request_path = '/users/self/verify'
        self.method = 'GET'
        self.auth = auth
        self.auth_dict = {}  # {ws: auth_result: True, account_id: ''}
        self.account_dict = {}
        self.non_sub_requests = []

    def init_auth_params(self, account_id, auth_params):
        if account_id not in self.account_dict.keys():
            self.account_dict[account_id] = auth_params

    def _get_timestamp(self):
        d = datetime.datetime.now()
        timestamp = time.mktime(d.timetuple())

        return timestamp

    def _pre_hash(self, _timestamp, method, body):
        return str(_timestamp) + str.upper(method) + self.request_path + body

    def _create_signature(self, method, params, secret_key):
        # sign=CryptoJS.enc.Base64.Stringify(CryptoJS.HmacSHA256(timestamp +'GET'+ '/users/self/verify', secret))
        _timestamp = self._get_timestamp()
        body = json.dumps(params) if method == 'POST' else ""
        message = self._pre_hash(_timestamp, method, body)  # 拼接字符串
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        signature = base64.b64encode(d).decode()

        return signature

    def _auth_with_params(self, method, access_key, secret_key, passphrase, params):
        _timestamp = self._get_timestamp()
        _sign = self._create_signature(method, params, secret_key)

        auth_params = {
            'op': 'login',
            'args':
                [
                    {
                        "apiKey": access_key,
                        "passphrase": passphrase,
                        "timestamp": _timestamp,
                        "sign": _sign
                    }
                ]
        }
        return auth_params

    def _login(self, ws, method, access_key, secret_key, passphrase, params):
        auth_params = self._auth_with_params(method, access_key, secret_key, passphrase, params)
        ws.send(json.dumps(auth_params))

    def handler_callback_message(self, message):
        pass

    def get_auth_value(self, ws, field):
        ws_dict = self.auth_dict.get(ws, {})
        value = ws_dict.get(field, None)
        return value

    def clear(self, ws):
        account_id = self.get_auth_value(ws, 'account_id')

        if ws in self.auth_dict.keys():
            self.auth_dict.pop(ws)
        if account_id in self.account_dict.keys():
            self.account_dict.pop(account_id)

    def on_message(self, ws, message):
        print('开始接收数据, on_message')
        print(type(message), message)

        if message != 'pong':
            message = json.loads(message)
            event = message.get('event', '')
            if self.auth:  # 需要认证
                if event == 'login':  # {"event":"login", "msg" : "", "code": "0"}
                    code = message.get('code', '')
                    msg = message.get('msg', '')

                    print('-------- 校验 ------', code, msg, event)
                    if str(code) == '0':
                        self.auth_dict[ws]['auth_result'] = True

            if ws in self.auth_dict.keys():
                message['account_id'] = self.get_auth_value(ws, 'account_id')
            self.handler_callback_message(message)

    def on_error(self, ws, error):
        print('on_error', error)
        self.clear(ws)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")
        self.clear(ws)

    def on_open(self, ws):
        print('on_open')
        # -------------------- 校验 --------------------------
        if self.auth:  # 私有频道
            def run():
                account_id = self.get_auth_value(ws, 'account_id')
                if not self.get_auth_value(ws, 'auth_result'):  # {ws: {'auth_result': '', account_id: ''}}
                    # 取对应的请求参数
                    auth_params = self.account_dict.get(account_id, {})

                    access_key = auth_params.get('access_key', '')
                    secret_key = auth_params.get('secret_key', '')
                    passphrase = auth_params.get('pass_phrase', '')
                    self._login(ws, self.method, access_key, secret_key, passphrase, {})

                    # 等待验证通过
                    timeout = 0
                    while True:
                        auth_result = self.get_auth_value(ws, 'auth_result')
                        if auth_result:
                            break

                        if timeout == 60:
                            ws.close()
                            break
                        timeout += 1
                        time.sleep(1)

                    if self.get_auth_value(ws, 'auth_result'):
                        for sub_request in self.sub_requests:
                            print('发送订阅消息, ', sub_request)
                            ws.send(json.dumps(sub_request))
                            time.sleep(0.05)

                        # 发送心跳消息
                        timeout = 0
                        while True:
                            if timeout == 25:
                                print('发送心跳消息, ping', account_id, timeout)
                                ws.send('ping')
                                timeout = 0

                            if account_id not in self.account_dict.keys():  # 针对删除的账户, 线程退出
                                ws.close()
                                break
                            timeout += 1
                            time.sleep(1)

        else:
            def run():
                # 发送心跳消息
                timeout = 0
                while True:
                    if self.sub_requests:
                        for sub_request in self.sub_requests:
                            print('发送订阅消息, ', sub_request)
                            ws.send(json.dumps(sub_request))
                            time.sleep(0.05)
                        self.sub_requests = []

                    if timeout == 25:
                        print('发送心跳消息, ping', timeout)
                        ws.send('ping')
                        timeout = 0

                    if self.non_sub_requests:
                        for non_sub_request in self.non_sub_requests:
                            print('取消订阅, ', non_sub_request)
                            ws.send(json.dumps(non_sub_request))
                            time.sleep(0.05)
                        self.non_sub_requests = []

                    timeout += 1
                    time.sleep(1)

        threading.Thread(target=run, args=()).start()

    def on_ping(self, ws):
        print('on_ping')

    def on_pong(self, ws):
        print('on_pong')

    def _subscribe(self, account_id=None):
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(url=self.sub_url,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_ping=self.on_ping,
                                    on_pong=self.on_pong)

        if account_id: self.auth_dict[ws] = {'account_id': account_id}  # 注册绑定account_id
        ws.run_forever()

    def main(self, account_id=None):
        threading.Thread(target=self._subscribe, args=(account_id,)).start()


if __name__ == "__main__":
    pass
