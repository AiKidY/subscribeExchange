#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subscribe.ok.v3.publicchannel import PublicChannel

# 订阅公有频道
if __name__ == '__main__':
    obj = PublicChannel()
    # sub_requests = [
    #     {"op": "subscribe", "args": ["swap/ticker:BTC-USD-SWAP"]}
    # ]
    obj.sub()
