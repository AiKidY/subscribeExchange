#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from subscribe.ok.v5.privatechannel import PrivateChannel

# 订阅私有频道
if __name__ == '__main__':
    obj = PrivateChannel()
    # sub_requests = [{
    #     "op": "subscribe",
    #     "args": [{
    #         "channel": "balance_and_position"
    #     }]
    # }]
    obj.sub()
