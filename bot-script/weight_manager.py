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

class WeightManager(AsyncManager):
    # グローバル共有のインスタンスを保持するクラス変数
    _instance: object = None

    _db_columns_dict = {
        'dt': ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
        's': ('symbol', 'TEXT', 'NOT NULL'),
        'w': ('weight', 'NUMERIC', 'NOT NULL')
    }

    def __init__(self, params: dict = None):
        """
        WeightManagerコンストラクタ
        
        Parameters
        ----------
        params : dict
            (必須) 初期化パラメータが入った辞書
        params['timebar_db_name'] : str
            (必須) タイムバーを読み込むデータベース名
        params['timebar_table_name'] : str
            (必須) タイムバーを読み込むテーブル名
        params['components_count'] : int
            (必須) ポートフォリオに含まれる銘柄数
        params['risk_aversion'] : float
            (必須) リスク回避度
        params['l2_gamma'] : float
            (必須) L2正規化用パラメータ
        params['rebalance_interval'] : timedelta
            (必須) リバランス時間間隔
        params['rebalance_calc_range'] : timedelta
            (必須) 1回のポートフォリオ計算時の参照範囲
        """
        assert params['timebar_db_name'] is not None
        assert params['timebar_table_name'] is not None
        assert params['components_count'] is not None
        assert params['risk_aversion'] is not None
        assert params['l2_gamma'] is not None
        assert params['rebalance_interval'] is not None
        assert params['rebalance_calc_range'] is not None

        if WeightManager._instance is None:
            self._timebar_db_name: str = params['timebar_db_name']
            self._timebar_table_name: str = params['timebar_table_name']
            self._components_count: int = params['components_count']
            self._risk_aversion: float = params['risk_aversion']
            self._l2_gamma: float = params['l2_gamma']
            self._rebalance_interval: timedelta = params['rebalance_interval']
            self._rebalance_calc_range: timedelta = params['rebalance_calc_range']

            self._weights: pd.Series = None
            self._last_rebalance_idx = 0

            WeightManager._instance = self
            WeightManager.init_database()
    
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
        assert WeightManager._instance is not None
        assert TimebarManager._instance._timebar_interval is not None

        _interval_str = TimebarManager.get_interval_str(TimebarManager._timebar_interval)

        return f'{ExchangeManager.get_exchange_name()}_weight'.lower()

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
        assert WeightManager._instance is not None

        _instance: WeightManager = WeightManager._instance
        _table_name: str = WeightManager.get_table_name()

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
                
        # テーブルそのものがないケース
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
        TimebarManagerの非同期タスクループ起動用メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert WeightManager._instance is not None

        _instance: WeightManager = WeightManager._instance

        # Kafka producerを起動する
        cls._kafka_producer = AIOKafkaProducer(bootstrap_servers = 'kafka:9092')
        await cls._kafka_producer.start()

        cls._kafka_consumer = AIOKafkaConsumer('TimebarManager', bootstrap_servers = 'kafka:9092', group_id = 'group')
        await cls._kafka_consumer.start()

        # オーダー情報をwebsocketから受け取りログをDBに保存する非同期タスクを起動する
        asyncio.create_task(_instance._wait_timebar_async())
    
    async def _wait_timebar_async(self):
        while True:
            try:
                _msg_dict = await self._kafka_consumer.getmany(timeout_ms=100)
            except OffsetOutOfRangeError as err:
                tps = err.args[0].keys()
                await self._kafka_consumer.seek_to_beginning(*tps)
                continue
            except Exception as e:
                AsyncManager.log_error(f'WeightManager._wait_timebar_async() : Kafka consume failed. Exception {e}')
                continue
            
            if len(_msg_dict) > 0 or self._weights is None:
                for k, v in _msg_dict.items():
                    for msg in v:
                        AsyncManager.log_info(f'consumed: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}')

                # 時間足がアップデートされているので、ウェイト計算を行う
                await self._calc_weight()

    async def _prepare_dataframes(self, df: pd.DataFrame = None, rows: int = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        TimebarManagerから受け取ったタイムバーのDataFrameをポートフォリオ計算に利用するデータフレームに変換して返す
        
        Parameters
        ----------
        df : pd.DataFrame
            (必須) ポートフォリオ計算区間の全銘柄タイムバー

        Returns
        -------
        _df_close : pd.DataFrame
            全銘柄のクローズ系列が入ったDataFrame (行は時間で列は銘柄)
        _df_dollar_volume_sma : pd.DataFrame
            全銘柄のドル建て取引ボリュームの移動平均が入ったDataFrame （行は時間で列は銘柄）
        """
        assert df is not None
        assert rows is not None

        # 取引可能な銘柄だけを抽出するために、取引可能なシンボルを取得
        await ExchangeManager.update_exchangeinfo_async()
        _all_symbols = ExchangeManager.get_all_symbols()

        # dfにUSDT/USDTを追加
        _unique_datetime = df['datetime'].unique()
        _df_usdt = pd.DataFrame(index=_unique_datetime, columns= ['symbol', 'close']).rename_axis('datetime').reset_index()
        _df_usdt.loc[:, 'symbol'] = 'USDTUSDT'
        _df_usdt.loc[:, 'close'] = 1.0
        _df_usdt.loc[:, 'quote_volume'] = 1.0
        _df_usdt.reset_index(inplace = True)
        df = pd.concat([df, _df_usdt]).sort_values(by=['datetime', 'symbol'])

        # ポートフォリオ計算に利用するクローズとボリューム用のデータフレームを準備
        _df_close = df.pivot(index = 'datetime', columns = 'symbol', values = 'close').astype(float)
        _df_close = _df_close.loc[:, _all_symbols]
        _df_quote_volume = df.pivot(index = 'datetime', columns = 'symbol', values='quote_volume').astype(float).fillna(0)
        _necessary_rows = int(self._rebalance_calc_range.total_seconds() // TimebarManager._timebar_interval.total_seconds())
        _df_quote_volume_sma = _df_quote_volume.apply(lambda rows: talib.SMA(rows, _necessary_rows))
        _df_quote_volume_sma = _df_quote_volume_sma.loc[:, _all_symbols]

        return (_df_close.iloc[-rows:, :],  _df_quote_volume_sma.iloc[-rows:, :])

    async def _calc_weight(self):
        _now_datetime = datetime.now(timezone.utc)
        _now_timestamp = _now_datetime.timestamp()
        _rebalance_idx: int = int(_now_timestamp // self._rebalance_interval.total_seconds())

        # ウェイト計算を実施すべきか否かを判断するフラグを計算    
        _flag_rebalance = _rebalance_idx != self._last_rebalance_idx or self._weights is None

        if _flag_rebalance is False:
            AsyncManager.log_info('WeightManager._calc_weight() : Skipping this timebar')
            return
        
        # ウェイト計算の実行に必要なデータフレームを作成
        _to_datetime = datetime.fromtimestamp(_rebalance_idx * self._rebalance_interval.total_seconds(), tz = timezone.utc)
        _from_datetime = _to_datetime - self._rebalance_calc_range
        _sql = f'SELECT datetime, symbol, close, quote_volume FROM {self._timebar_table_name} WHERE datetime BETWEEN \'{_from_datetime}\' AND \'{_to_datetime}\' ORDER BY datetime ASC, symbol ASC'
        try:
            _df = TimescaleDBManager.read_sql_query(_sql, self._timebar_db_name)
        except Exception as e:
            AsyncManager.log_error(f'WeightManager.calc_portfolio_weight() : Cannot read timebar from DB. {e}')
            return
        
        _necessary_rows = int(self._rebalance_calc_range.total_seconds() // TimebarManager._timebar_interval.total_seconds())
        _df_close, _df_quote_volume_sma = await self._prepare_dataframes(_df, _necessary_rows)

        # まだポートフォリオ計算に必要なclose系列の長さがない場合は、USDT 100%のウェイトを_df_target_weightに設定してウェイト計算を終了する
        if _df_close.shape[0] != _necessary_rows:
            AsyncManager.log_warning(f'WeightManager._calc_weight() : Short or long dataframe is given. {_df_close.shape}.')
            _all_symbols = _df_close.columns.unique()
            self._weights = pd.Series(Decimal(0), index = _all_symbols)
            self._weights['USDTUSDT'] = Decimal(1)
            self._last_rebalance_idx = _rebalance_idx
            return True
        
        if _flag_rebalance:
            # 平均取引ボリュームに基づいて銘柄を選択する
            _volume_rank = _df_quote_volume_sma.iloc[-1].rank(ascending = False)
            self._components = list(_volume_rank[_volume_rank <= self._components_count].index.values)
            AsyncManager.log_info(f'WeightManager._calc_weight() : Components update performed.\n{self._components}')

            # タイムバーから全シンボルを取得し、ウェイト用のSeriesを準備
            _all_symbols = _df_close.columns.unique()
            self._weights = pd.Series(Decimal(0), index = _all_symbols)

            # ポートフォリオウェイト計算用のクローズ系列を抽出する
            _df_close_window = _df_close.loc[:, self._components]

            # ここよりPyPortfolioOptによるポートフォリオウェイト計算
            _mu = mean_historical_return(_df_close_window)
            _historical_returns = returns_from_prices(_df_close_window).fillna(0)

            _S = CovarianceShrinkage(_historical_returns, returns_data=True).ledoit_wolf()
            _ef = EfficientFrontier(_mu, _S, weight_bounds = (-1, 1))
            _ef.add_objective(objective_functions.L2_reg, gamma = self._l2_gamma) # L2正則化を入れてひとつの銘柄にウェイトが集中するのを防ぐ

            # メモ : 負のポジションを許し、ポジションの絶対値の合計を1以下にするためにpyportfoliooptの_make_weight_sum_constraint関数に変更を加えている
            #def _make_weight_sum_constraint(self, is_market_neutral):
            #    ...
            #    else:
            #        # Check negative bound
            #        negative_possible = np.any(self._lower_bounds < 0)
            #        if negative_possible:
            #            # Use norm1 as position constraint
            #            self.add_constraint(lambda w: cp.sum(cp.abs(w)) <= 1)
            #        else: 
            #            self.add_constraint(lambda w: cp.sum(w) == 1)
            #    self._market_neutral = is_market_neutral

            # ウェイトの計算、記録
            try:
                _weights = _ef.max_quadratic_utility(risk_aversion = self._risk_aversion)
                _cleaned_weights = _ef.clean_weights()
            except BaseException as e:
                # 例外発生時にはUSDTUSDTにすべてのウェイトを割り当てる
                _cleaned_weights = { 'USDTUSDT': Decimal(1) }

            for k, v in _cleaned_weights.items():
                self._weights[k] = Decimal(v)

            # ウェイトの正規化を行う
            if np.count_nonzero(self._weights) <= 1:
                # 構成銘柄が1つしかない場合は、足りないウェイトをUSDT/USDTに足す
                self._weights['USDTUSDT'] += (Decimal(1) - self._weights.abs().sum())
            elif self._weights.abs().sum() > 0:
                # 構成銘柄が複数ある場合は、正規化を行う
                self._weights = self._weights / self._weights.abs().sum()
            
            self._last_rebalance_idx = _rebalance_idx

            # ウェイトをDBに記録する
            _df = self._weights.to_frame().reset_index()
            _df.columns = ['symbol', 'weight']
            _df.loc[:, 'datetime'] = _to_datetime

            TimescaleDBManager.df_to_sql(_df, 'WeightManager', self.get_table_name(), 'append')

            AsyncManager.log_info(f'WeightManager._calc_weight() : new weight = {self._weights[self._weights != 0]}')
            AsyncManager.log_info(f'WeightManager._calc_weight() : new weight abs sum = {np.sum(np.abs(self._weights))}')


if __name__ == "__main__":
    # タイムバーのダウンロードをトリガーにウェイトを計算するプログラム
    from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis
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

        wm_config = {
            'timebar_db_name': 'TimebarManager',
            'timebar_table_name': 'binanceusdm_timebar_5m',
            'components_count': 16,
            'risk_aversion': 2.0,
            'l2_gamma': 0.02,
            'rebalance_interval': timedelta(minutes = 5),
            'rebalance_calc_range': timedelta(days = 2)
        }
            
        WeightManager(wm_config)
        await WeightManager.run_async()

        while True:
            await asyncio.sleep(60.0)

    try:
        asyncio.run(async_task())
    except KeyboardInterrupt:
        pass
