#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subscribe.ok.v3.privatechannel import PrivateChannel

# 订阅公有频道
if __name__ == '__main__':
    obj = PrivateChannel()
    # sub_requests = [
    #     {
    #         'op': 'subscribe',
    #         'args': ["futures/account:BTC-USDT"]
    #     }
    # ]
    obj.sub()
