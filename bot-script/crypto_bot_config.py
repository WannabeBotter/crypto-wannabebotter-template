from os import environ

# TimescaleDBのパラメータ
pg_config = {
    'user': environ['POSTGRES_USER'],
    'password': environ['POSTGRES_PASSWORD'],
    'host': environ['POSTGRES_HOST'],
    'port': environ['POSTGRES_PORT']
}

binance_testnet_config = {
    'exchange_name': 'binanceusdm(testnet)',
    'rest_baseurl': 'https://testnet.binancefuture.com',
    'ws_baseurl': 'wss://stream.binancefuture.com',
}

binance_config = {
    'exchange_name': 'binanceusdm',
    'rest_baseurl': 'https://fapi.binance.com',
    'ws_baseurl': 'wss://fstream.binance.com',
}

# PyBottersのパラメータ
pybotters_apis = {
    'binance': [environ['BINANCE_APIKEY'], environ['BINANCE_APISECRET']],
    'binance_testnet': [environ['BINANCE_TESTNET_APIKEY'], environ['BINANCE_TESTNET_APISECRET']]
}