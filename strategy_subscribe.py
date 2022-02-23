# -*- coding: UTF-8 -*-
import json
import time
import multiprocessing
from threading import Thread

from utils.config import config
from strategy_order.utils.const import const
from utils.redis_util import master_redis_client
from strategy_order.base.send_email import SendOrderDetailEmail
from subscribe.hb import publicchannel as publicchannel_hb, privatechannel as privatechannel_hb
from subscribe.ok.v3 import publicchannel as publicchannel_ok_v3, privatechannel as privatechannel_ok_v3
from subscribe.ok.v5 import publicchannel as publicchannel_ok_v5, privatechannel as privatechannel_ok_v5


class Subscribe:
    def __init__(self):
        self.send_detail_email = SendOrderDetailEmail()
        self.process_dict = dict()
        self.init_process_dict()
        self.queue = multiprocessing.Queue()
        self.interval_time = 60  # 进程中断重启间隔
        self.service_name = config.service_name  # 当前环境
        self.pool = multiprocessing.Pool(processes=len(self.process_dict.keys()),
                                         maxtasksperchild=1)

    @staticmethod
    def get_system_param():
        system_param = {}
        try:
            system_param = master_redis_client.get(const.SYSTEM_PARAM)
            if system_param:
                system_param = json.loads(system_param)
        except Exception as e:
            print(e)
        finally:
            return system_param if system_param else {}

    def init_process_dict(self):
        self.process_dict = {
            'publicchannel_spot_hb': {'obj': publicchannel_hb.PublicChannel().sub_spot, 'restart': False},
            'publicchannel_futures_hb': {'obj': publicchannel_hb.PublicChannel().sub_futures, 'restart': False},
            'privatechannel_hb': {'obj': privatechannel_hb.PrivateChannel().sub, 'restart': False},
            # 'publicchannel_ok_v3': {'obj': publicchannel_ok_v3.PublicChannel().sub, 'restart': False}, # TODO v3行情无需订阅
            'privatechannel_ok_v3': {'obj': privatechannel_ok_v3.PrivateChannel().sub, 'restart': False},
            'publicchannel_ok_v5': {'obj': publicchannel_ok_v5.PublicChannel().sub, 'restart': False},
            'privatechannel_ok_v5': {'obj': privatechannel_ok_v5.PrivateChannel().sub, 'restart': False}
        }

    def callback(self, callback):
        print('callback, ', callback)
        process_name, process_pid = callback
        restart = self.process_dict.get(process_name, {}).get('restart')
        if restart:
            print('进程中断, 等待{}秒后再重启, process_name: {}, process_pid: {}'.format(self.interval_time, process_pid,
                                                                              process_name))
            time.sleep(self.interval_time)
        Thread(target=self.handler, args=(callback,)).start()

    def handler(self, callback):
        process_name, process_pid = callback
        self.process_dict[process_name]['restart'] = True

        # ---------------------- 发送服务中断邮件 --------------------------------
        email_content = '环境名称: {}, 进程名称: {}, 进程pid: {}挂了, 请检查...'.format(self.service_name, process_name, process_pid)
        # email_list = ['2693917459@qq.com', 'liuyong@wescxx.com']  # TODO 测试
        email_list = self.get_system_param().get('maintenance_email', '').split(';')
        title = '订阅进程服务中断提醒'
        self.send_email(email_content, email_list, title)
        print('发送进程中断提醒邮件成功, ', email_list, email_content, title)
        # ---------------------- 发送服务中断邮件 --------------------------------

        self.restart_process(process_name, process_pid)

    def restart_process(self, process_name, process_pid):
        print('----- restart_process, process_name: {}, process_pid: {} -----'.format(process_name, process_pid))
        process_obj = self.process_dict.get(process_name, {}).get('obj')
        if process_obj:
            # ctx = multiprocessing.get_context('spawn')
            # pool = ctx.Pool()
            p = self.apply_async(self.pool, process_obj, process_name)

            # ---------------------- 发送服务恢复邮件 --------------------------------
            email_content = '环境名称: {}, 进程名称: {}, 进程pid: {}重启成功'.format(self.service_name, process_name, process_pid)
            # email_list = ['2693917459@qq.com', 'liuyong@wescxx.com']  # TODO 测试
            email_list = self.get_system_param().get('maintenance_email', '').split(';')
            title = '订阅进程服务恢复提醒'
            self.send_email(email_content, email_list, title)
            print('发送进程重启成功邮件成功, ', email_list, email_content, title)
            # ---------------------- 发送服务恢复邮件 --------------------------------
            p.wait()

    def error_callback(self, msg):
        print('error_callback, ', msg)

    def send_email(self, email_content='', email_list=None, title=''):
        try:
            self.send_detail_email.send_message(email_content, email_list, title)
        except Exception as e:
            print(e)

    def start(self):
        try:
            p_list = []
            for process_name, process_value in self.process_dict.items():
                process_obj = process_value.get('obj')
                p = self.apply_async(self.pool, process_obj, process_name)
                print('进程启动成功, process_name: {}'.format(process_name))
                p_list.append(p)

            for p in p_list:
                p.wait()
        except Exception as e:
            print(e)

    def apply_async(self, pool, process_obj, process_name):
        p = pool.apply_async(func=process_obj, args=(process_name,), callback=self.callback,
                             error_callback=self.error_callback)
        return p


if __name__ == '__main__':
    multiprocessing.set_start_method('forkserver')
    multiprocessing.freeze_support()
    obj = Subscribe()
    obj.start()
