import gc
import asyncio
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Tuple

import pandas as pd
import numpy as np

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import OffsetOutOfRangeError

import talib

from pypfopt.expected_returns import mean_historical_return, returns_from_prices
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import objective_functions

from async_manager import AsyncManager
from timescaledb_manager import TimescaleDBManager
from pybotters_manager import PyBottersManager
from exchange_manager import ExchangeManager
from timebar_manager import TimebarManager
from weight_manager import WeightManager

class TradeManager:
    # グローバル共有のインスタンスを保持するクラス変数
    _instance: object = None

    # DB内のカラム名の対象用の辞書
    _db_columns_dict = {
        0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
        1: ('symbol', 'TEXT', 'NOT NULL'),
        2: ('base_amount', 'NUMERIC', 'NOT NULL'),
        3: ('latest_close', 'NUMERIC', 'NOT NULL'),
        4: ('latest_best_bid', 'NUMERIC', 'NOT NULL'),
        5: ('latest_best_ask', 'NUMERIC', 'NOT NULL'),
    }

    def __init__(self, params: dict = None):
        """
        TradeManagerコンストラクタ
        
        Parameters
        ----------
        params : dict
            (必須) 初期化パラメータが入った辞書
        params['weight_table_name'] : str
            (必須) ウェイトテーブル名
        params['weight_db_name'] : str
            (必須) ウェイトDB名
        params['trade_interval'] : timedelta
            (必須) トレードを実施する間隔
        params['rebalance_time'] : timedelta
            (必須) リバランス期間の長さ
        params['components_num'] : int
            (必須) 最大の銘柄数
        """
        assert params['weight_table_name'] is not None
        assert params['weight_db_name'] is not None
        assert params['trade_interval'] is not None
        assert params['rebalance_time'] is not None

        self._weight_table_name = params['weight_table_name']
        self._weight_db_name = params['weight_db_name']
        self._trade_interval = params['trade_interval']
        self._rebalance_time = params['rebalance_time']
        self._components_num = params['components_num']

        # 目標ウェイト等を保存する変数を未定義状態に
        self._target_weight = None
        self._target_datetime = None
        self._origin_datetime = None

        TradeManager._instance = self

    @classmethod
    def get_table_name(cls) -> str:
        """
        このマネージャーが使うテーブル名を取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        テーブル名 : str
        """
        assert TradeManager()._instance is not None
        assert TimebarManager._instance._timebar_interval is not None

        _interval_str = TimebarManager.get_interval_str(TimebarManager._timebar_interval)

        return f'{ExchangeManager.get_exchange_name()}_place_order_{_interval_str}'.lower()

    @classmethod
    def init_database(cls, force = False):
        """
        ウェイト情報用のDBとテーブルを初期化する
        
        Parameters
        ----------
        force : bool
            強制的にテーブルを初期化するか否か (デフォルト値はfalse)

        Returns
        ----------
        なし。失敗した場合は例外をRaiseする
        """
        assert TradeManager._instance is not None

        _instance: TradeManager = TradeManager._instance
        _table_name: str = TradeManager.get_table_name()

        try:
            # このマネージャー専用のデータベースの作成を試みる。すでにある場合はそのまま処理を続行する
            TimescaleDBManager.init_database(cls.__name__)

            # データベースの中にテーブルが存在しているか確認する
            _df = TimescaleDBManager.read_sql_query(f"select * from information_schema.tables where table_name='{_table_name}'", cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'WeightManager.init_database(database_name = {cls.__name__}, table_name = {_table_name}) : Database init failed. Exception {e}')
            raise(e)
        
        if len(_df.index) > 0 and force == False:
            return

        _columns_str_list = [f'{v[0]} {v[1]} {v[2]}' for k, v in _instance._db_columns_dict.items()]
        _columns_str = ', '.join(_columns_str_list)
        
        # 目標ウェイト記録テーブルを作成
        _sql = (f'DROP TABLE IF EXISTS "{_table_name}" CASCADE;'
                f' CREATE TABLE IF NOT EXISTS "{_table_name}" ({_columns_str}, UNIQUE(datetime, symbol));'
                f' CREATE INDEX ON "{_table_name}" (datetime DESC);'
                f" SELECT create_hypertable ('{_table_name}', 'datetime');")
        
        try:
            # テーブルの削除と再作成を試みる
            TimescaleDBManager.execute_sql(_sql, cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'TimebarManager.init_database(database_name = {cls.__name__}, table_name = {_table_name}) : Create table failed. Exception {e}')
            raise(e)

    @classmethod
    async def run_async(cls) -> None:
        """
        TradeManagerの非同期タスクループ起動用メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert TradeManager._instance is not None

        _instance: TradeManager = TradeManager._instance

        # Kafka producerを起動する
        cls._kafka_producer = AIOKafkaProducer(bootstrap_servers = 'kafka:9092')
        await cls._kafka_producer.start()

        cls._kafka_consumer = AIOKafkaConsumer('WeightManager', bootstrap_servers = 'kafka:9092', group_id = 'group')
        await cls._kafka_consumer.start()

        # ウェイト情報をkafkaのシグナルに従って読み込み、トレード設定をする非同期タスクを起動する
        asyncio.create_task(_instance._wait_weight_async())

        # 指定された時間間隔で注文を試みる非同期タスクを起動する
        #asyncio.create_task(_instance._wait_timebar_async())

    async def _wait_weight_async(self):
        # 初回の目標ウェイトを更新
        self._update_target_weight()

        while True:
            try:
                _msg_dict = await self._kafka_consumer.getmany(timeout_ms=100)
            except OffsetOutOfRangeError as err:
                tps = err.args[0].keys()
                await self._kafka_consumer.seek_to_beginning(*tps)
                continue
            except Exception as e:
                AsyncManager.log_error(f'TradeManager._wait_timebar_async() : Kafka consume failed. Exception {e}')
                continue
            
            if len(_msg_dict) > 0 or self._target_weight is None:
                for k, v in _msg_dict.items():
                    for msg in v:
                        AsyncManager.log_info(f'consumed: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}')
                
                # 目標ウェイトを更新
                self._update_target_weight()
    
    def _update_target_weight(self):
        # 最新のウェイトを読み込む
        _sql = f"SELECT * FROM {self._weight_table_name} ORDER BY datetime DESC, symbol ASC LIMIT {self._components_num}"
        try:
            _df = TimescaleDBManager.read_sql_query(_sql, self._weight_db_name)
            _df = _df.loc[_df.loc[:, 'datetime'] == _df.iloc[0, _df.columns.get_loc('datetime')], :]
            AsyncManager.log_info(_df)
        except Exception as e:
            AsyncManager.log_error(f'WeightManager.calc_portfolio_weight() : Cannot read weight from DB. {e}')
            return
        
        self._target_datetime = _df.iloc[0, _df.columns.get_loc('datetime')].to_pydatetime() + self._rebalance_time

        _df = _df.reset_index(drop = True).set_index('symbol')
        self._target_weight = _df.loc[:, 'weight']

if __name__ == "__main__":
    # 一定間隔でトレードをしながら、ウェイトの更新をトリガーに目標ウェイトを更新し続けるプログラム
    from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis, wm_config, tm_config
    from logging import Logger, getLogger, basicConfig, Formatter
    import logging
    from rich.logging import RichHandler

    async def async_task():
        # AsyncManagerの初期化
        _richhandler = RichHandler(rich_tracebacks = True)
        _richhandler.setFormatter(logging.Formatter('%(message)s'))
        basicConfig(level = logging.INFO, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
        _logger: Logger = getLogger('rich')
        AsyncManager.set_logger(_logger)

        # TimebarManagerの初期化前に、TimescaleDBManagerの初期化が必要
        TimescaleDBManager(pg_config)

        # TimebarManagerの初期化前に、PyBottersManagerの初期化が必要
        _pybotters_params = binance_config.copy()
        _pybotters_params['apis'] = pybotters_apis.copy()
        PyBottersManager(_pybotters_params)

        # タイムバーをダウンロードするだけなら、run_asyncを読んでWebsocket APIからポジション情報等をダウンロードする必要はない
        _exchange_config = binance_config.copy()
        ExchangeManager(_exchange_config)

        # TimebarManagerの初期化
        _timebar_params = {
            'timebar_interval': timedelta(minutes = 5)
        }
        TimebarManager(_timebar_params)

        # WeightManagerの初期化
        WeightManager(wm_config)

        TradeManager(tm_config)
        await TradeManager.run_async()

        while True:
            await asyncio.sleep(60.0)

    try:
        asyncio.run(async_task())
    except KeyboardInterrupt:
        pass
