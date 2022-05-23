from os import environ
from datetime import timedelta

# TimescaleDBのパラメータ
pg_config = {
    'user': environ['POSTGRES_USER'],
    'password': environ['POSTGRES_PASSWORD'],
    'host': environ['POSTGRES_HOST'],
    'port': environ['POSTGRES_PORT']
}

# ExchangeManagerのパラメータ
binance_testnet_config = {
    'exchange_name': 'binanceusdm-testnet',
    'rest_baseurl': 'https://testnet.binancefuture.com',
    'ws_baseurl': 'wss://stream.binancefuture.com',
}

# ExchangeManagerのパラメータ
binance_config = {
    'exchange_name': 'binanceusdm-mainnet',
    'rest_baseurl': 'https://fapi.binance.com',
    'ws_baseurl': 'wss://fstream.binance.com',
}

# PyBottersManagerのパラメータ
pybotters_apis = {
    'binance': [environ['BINANCE_APIKEY'], environ['BINANCE_APISECRET']],
    'binance_testnet': [environ['BINANCE_TESTNET_APIKEY'], environ['BINANCE_TESTNET_APISECRET']]
}

#WeightManagerのパラメータ
wm_config = {
    'prohibit_symbol': ['LUNAUSDT'],
    'timebar_db_name': 'TimebarManager',
    'timebar_table_name': 'binanceusdm_timebar_5m',
    'components_num': 16,
    'risk_aversion': 2.0,
    'l2_gamma': 0.02,
    'rebalance_interval': timedelta(days = 1),
    'rebalance_calc_range': timedelta(days = 2)
}

# TradeManagerのパラメータ
tm_config = {
    'weight_table_name': 'binanceusdm_weight_5m',
    'weight_db_name': 'WeightManager',
    'trade_interval': timedelta(minutes = 5),
    'rebalance_time': timedelta(hours = 2),
    'components_num': wm_config['components_num']    
}
