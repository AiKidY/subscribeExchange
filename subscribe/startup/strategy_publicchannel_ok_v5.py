#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subscribe.ok.v5.publicchannel import PublicChannel

# 订阅公有频道
if __name__ == '__main__':
    obj = PublicChannel()
    # sub_requests = [{
    #     "op": "subscribe",
    #     "args": [{
    #         "channel": "tickers",
    #         "instId": "BTC-USD-210604"
    #     }]
    # }]
    obj.sub()
