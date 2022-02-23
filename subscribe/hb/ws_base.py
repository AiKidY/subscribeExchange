#!/usr/bin/env python
import websocket
import json
import datetime
import hmac
import base64
import threading
import time
import hashlib
import urllib.parse
import gzip


class WebsocketBase:
    def __init__(self, auth):
        self.sub_url = ''
        self.sub_requests = []
        self.method = 'GET'
        self.auth = auth
        self.auth_dict = {}  # {ws: auth_result: True, account_id: ''}
        self.account_dict = {}
        self.non_sub_requests = []

    def init_auth_params(self, account_id, auth_params):
        if account_id not in self.account_dict.keys():
            self.account_dict[account_id] = auth_params

    def _get_timestamp(self):
        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return timestamp

    def _create_signature(self, url, method, params, secret_key, _timestamp):
        host_url = urllib.parse.urlparse(url).hostname.lower()
        request_path = urllib.parse.urlparse(url).path.lower()

        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.parse.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = "\n".join(payload)
        payload = payload.encode(encoding="UTF8")
        secret_key = secret_key.encode(encoding="utf8")
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature

    def _auth_with_params(self, method, timestamp, api_key, passphrase, secret_key):
        data = {
            "AccessKeyId": api_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": timestamp
        }
        sign = self._create_signature(self.sub_url, method, data, secret_key, timestamp)

        data["op"] = "auth"
        data["type"] = "api"
        data["Signature"] = sign
        login_str = json.dumps(data)
        return login_str

    def _login(self, ws, method, access_key, secret_key, passphrase, params):
        timestamp = self._get_timestamp()
        auth_params = self._auth_with_params(method, timestamp, access_key, passphrase, secret_key)
        ws.send(auth_params)

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

    def __check(self, ws):
        account_id = self.get_auth_value(ws, 'account_id')
        if account_id in self.account_dict.keys():
            return True
        ws.close()

    def on_message(self, ws, message):
        print('开始接收数据, on_message')  # {'ping': 1623866484812}
        message = json.loads(gzip.decompress(message).decode())
        print(type(message), message)

        op = message.get('op', '')
        ping = message.get('ping', '')  # {'ping': 1623866692035}

        if op == 'ping':  # {'op': 'ping', 'ts': '1623862208024'} # 订单推送心跳
            pong_msg = {"op": "pong", "ts": message.get("ts")}
            if not self.auth:
                ws.send(json.dumps(pong_msg))
            elif self.auth and self.__check(ws):
                ws.send(json.dumps(pong_msg))
        elif ping:  # 市场行情心跳
            pong_msg = {"pong": ping}
            ws.send(json.dumps(pong_msg))
        else:
            if self.auth:  # 需要认证
                if op == 'auth':  # {'op': 'auth', 'type': 'api', 'err-code': 0, 'ts': 1623860122090, 'data': {'user-id': '13733006'}}
                    err_code = message.get('err-code', '')

                    print('-------- 校验 ------', err_code)
                    if str(err_code) == '0':
                        self.auth_dict[ws]['auth_result'] = True

            if ws in self.auth_dict.keys():
                message['account_id'] = self.get_auth_value(ws, 'account_id')

            self.handler_callback_message(message)

    def on_error(self, ws, error):
        print(error)
        self.clear(ws)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)
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
                        time.sleep(1)
                        timeout += 1

                    if self.get_auth_value(ws, 'auth_result'):
                        for sub_request in self.sub_requests:
                            print('发送订阅消息：', sub_request)
                            ws.send(json.dumps(sub_request))
                            time.sleep(0.05)

        else:
            def run():
                while True:
                    if self.sub_requests:
                        for sub_request in self.sub_requests:  # 增量订阅/全量订阅
                            print('发送订阅消息, ', sub_request)
                            ws.send(json.dumps(sub_request))
                            time.sleep(0.05)
                        self.sub_requests = []

                    if self.non_sub_requests:
                        for non_sub_request in self.non_sub_requests:  # 增量取消订阅/全量取消订阅
                            print('取消订阅, ', non_sub_request)
                            ws.send(json.dumps(non_sub_request))
                            time.sleep(0.05)
                        self.non_sub_requests = []

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
