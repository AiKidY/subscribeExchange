# -*- coding: UTF-8 -*-


class __const:
    # ok订阅(v3)
    WS_URL_OK_V3 = None

    ZMQ_POSITION_PORT_OK_V3 = None
    ZMQ_ASSETS_PORT_OK_V3 = None
    ZMQ_QUOTATION_PORT_OK_V3 = None

    # ok订阅(v5)
    WS_PUBLIC_CHANNEL_OK_V5 = None
    WS_PIRVATE_CHANNEL_OK_V5 = None
    WS_PUBLIC_SIMULATE_CHANNEL_OK_V5 = None
    WS_PIRVATE_SIMULATE_CHANNEL_OK_V5 = None

    WS_POSITION_PORT_OK_V5 = None

    ZMQ_QUOTATION_PORT_OK_V5 = None
    ZMQ_POSITION_PORT_OK_V5 = None
    ZMQ_ASSETS_PORT_OK_V5 = None

    # hb订阅
    WS_FUTURES_QUOTATION_URL_HB = None
    WS_SPOT_QUOTATION_URL_HB = None
    WS_FUTURES_ORDER_URL_HB = None

    ZMQ_FUTURES_QUOTATION_PORT_HB = None
    ZMQ_SPOT_QUOTATION_PORT_HB = None
    ZMQ_POSITION_PORT_HB = None
    ZMQ_ASSETS_PORT_HB = None

    ZMQ_QUOTATION_TIMEOUT = None
    ZMQ_POSITION_TIMEOUT = None
    ZMQ_ASSETS_TIMEOUT = None

    # WS
    WS_POSITION_PORT = None
    WS_ASSERTS_PORT = None
    WS_FUTURES_SPOT_YIELD_PORT = None

    class __ConstError(TypeError):
        pass

    class __ConstCaseError(__ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.__ConstError("can't change const %s" % name)
        if not name.isupper():
            raise self.__ConstCaseError('const name "%s" is not all uppercase' % name)
        self.__dict__[name] = value


const = __const()

# ok订阅(v3)
const.WS_URL_OK_V3 = 'wss://real.okex.com:8443/ws/v3'

const.ZMQ_QUOTATION_PORT_OK_V3 = 1111
const.ZMQ_POSITION_PORT_OK_V3 = 1112
const.ZMQ_ASSETS_PORT_OK_V3 = 1113

# ok订阅(v5)
const.WS_PUBLIC_CHANNEL_OK_V5 = 'wss://ws.okex.com:8443/ws/v5/public'
const.WS_PIRVATE_CHANNEL_OK_V5 = 'wss://ws.okex.com:8443/ws/v5/private'
const.WS_PUBLIC_SIMULATE_CHANNEL_OK_V5 = 'wss://ws.okex.com:8443/ws/v5/public?brokerId=9999'
const.WS_PIRVATE_SIMULATE_CHANNEL_OK_V5 = 'wss://ws.okex.com:8443/ws/v5/private?brokerId=9999'

const.ZMQ_QUOTATION_PORT_OK_V5 = 1114
const.ZMQ_POSITION_PORT_OK_V5 = 1115
const.ZMQ_ASSETS_PORT_OK_V5 = 1116

# hb订阅
const.WS_FUTURES_QUOTATION_URL_HB = 'wss://api.hbdm.com/ws'
const.WS_SPOT_QUOTATION_URL_HB = 'wss://api-aws.huobi.pro/ws'  # 行情
const.WS_FUTURES_ORDER_URL_HB = 'wss://api.hbdm.com/notification'  # 订单

const.ZMQ_FUTURES_QUOTATION_PORT_HB = 1117
const.ZMQ_SPOT_QUOTATION_PORT_HB = 1122
const.ZMQ_POSITION_PORT_HB = 1118
const.ZMQ_ASSETS_PORT_HB = 1119

const.ZMQ_QUOTATION_TIMEOUT = 150000
const.ZMQ_POSITION_TIMEOUT = 150000
const.ZMQ_ASSETS_TIMEOUT = 150000

# ws
const.WS_POSITION_PORT = 1121
const.WS_ASSERTS_PORT = 1120
const.WS_FUTURES_SPOT_YIELD_PORT = 6775
