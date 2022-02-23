#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import multiprocessing

from subscribe.hb.publicchannel import PublicChannel as publicchannel_hb
from multiprocessing import Pool


def callback(callback):
    print('callback,', callback)


def error_callback(msg):
    print('error_message, ', msg)


if __name__ == '__main__':
    multiprocessing.set_start_method('forkserver')
    process_dict = {
        'publicchannel_spot_hb': {'obj': publicchannel_hb.sub_spot},
        'publicchannel_futures_hb': {'obj': publicchannel_hb.sub_futures}
    }

    pool = Pool(processes=len(process_dict.keys()))

    for process_name, value in process_dict.items():
        func = value.get('obj')
        pool.apply_async(func=func, args=(process_name,), callback=callback, error_callback=error_callback)

    pool.close()
    pool.join()
