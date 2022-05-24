import asyncio
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta
from unicodedata import decimal

import pandas as pd

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import OffsetOutOfRangeError

import pybotters

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
    _db_order_log_columns_dict = {
        'order_attempt_log': {
            0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
            1: ('symbol', 'TEXT', 'NOT NULL'),
            2: ('base_amount', 'NUMERIC', 'NOT NULL'),
            3: ('close', 'NUMERIC', 'NOT NULL'),
            4: ('best_bid', 'NUMERIC', 'NOT NULL'),
            5: ('best_bid_qty', 'NUMERIC', 'NOT NULL'),
            6: ('best_ask', 'NUMERIC', 'NOT NULL'),
            7: ('best_ask_qty', 'NUMERIC', 'NOT NULL')
        },
        'current_weight': {
            0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
            1: ('symbol', 'TEXT', 'NOT NULL'),
            2: ('value', 'NUMERIC', 'NOT NULL')
        },
        'target_weight': {
            0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
            1: ('symbol', 'TEXT', 'NOT NULL'),
            2: ('value', 'NUMERIC', 'NOT NULL')
        },
        'current_value': {
            0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
            1: ('symbol', 'TEXT', 'NOT NULL'),
            2: ('value', 'NUMERIC', 'NOT NULL')
        },
        'current_upnl': {
            0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
            1: ('symbol', 'TEXT', 'NOT NULL'),
            2: ('value', 'NUMERIC', 'NOT NULL')
        }
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
        params['ws_baseurl'] : str
            (必須) WebsocketAPIのベースURL
        """
        assert params['weight_table_name'] is not None
        assert params['weight_db_name'] is not None
        assert params['trade_interval'] is not None
        assert params['rebalance_time'] is not None
        assert params['ws_baseurl'] is not None
        
        self._weight_table_name = params['weight_table_name']
        self._weight_db_name = params['weight_db_name']
        self._trade_interval = params['trade_interval']
        self._rebalance_time = params['rebalance_time']
        self._components_num = params['components_num']
        self._ws_baseurl = params['ws_baseurl']

        # 目標ウェイト等を保存する変数を未定義状態に
        self._target_weight: pd.Series = None
        self._target_datetime: datetime = None
        self._origin_datetime: datetime = None

        self._step_count: int = int(self._rebalance_time.total_seconds() // self._trade_interval.total_seconds())
        self._last_step = 0

        self._datastore = pybotters.BinanceDataStore()

        TradeManager._instance = self
        self.init_database()

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
        return ''
        
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
        assert TimebarManager._instance._timebar_interval is not None

        _instance: TradeManager = TradeManager._instance
        _exchange_name = ExchangeManager.get_exchange_name()
        _interval_str = TimebarManager.get_interval_str(TimebarManager._timebar_interval)

        try:
            # このマネージャー専用のデータベースの作成を試みる。すでにある場合はそのまま処理を続行する
            TimescaleDBManager.init_database(cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'WeightManager.init_database(database_name = {cls.__name__}, table_name = {_table_name}) : Database init failed. Exception {e}')
            raise(e)
        
        for _k, _v in _instance._db_order_log_columns_dict.items():
            _table_name = f'{_exchange_name}_{_k}_{_interval_str}'.lower()
            _columns_str_list = [f'{_v2[0]} {_v2[1]} {_v2[2]}' for _k2, _v2 in _v.items()]
            _columns_str = ', '.join(_columns_str_list)
            
            # 目標ウェイト記録テーブルを作成
            _sql = (f'DROP TABLE IF EXISTS "{_table_name}" CASCADE;'
                    f' CREATE TABLE IF NOT EXISTS "{_table_name}" ({_columns_str}, UNIQUE(datetime, symbol));'
                    f' CREATE INDEX ON "{_table_name}" (datetime DESC);'
                    f" SELECT create_hypertable ('{_table_name}', 'datetime');")
            
            try:
                # データベースの中にテーブルが存在しているか確認する
                _df = TimescaleDBManager.read_sql_query(f"SELECT * FROM information_schema.tables WHERE table_name='{_table_name}'", cls.__name__)
                if len(_df.index) > 0 and force == False:
                    continue

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
        _client: pybotters.Client = PyBottersManager.get_client()

        assert _client is not None

        # データストアを初期化する
        await _instance._datastore.initialize()

        # Best bid / askにsubscribeする
        asyncio.create_task(_client.ws_connect(f'{_instance._ws_baseurl}/ws/!bookTicker', hdlr_json = _instance._datastore.onmessage, heartbeat = 10.0))
        asyncio.create_task(_client.ws_connect(f'{_instance._ws_baseurl}/ws/!miniTicker@arr', hdlr_json = _instance._datastore.onmessage, heartbeat = 10.0))

        # Kafka producerを起動する
        cls._kafka_producer = AIOKafkaProducer(bootstrap_servers = 'kafka:9092')
        await cls._kafka_producer.start()

        cls._kafka_consumer = AIOKafkaConsumer(f'WeightManager.{ExchangeManager.get_exchange_name()}', bootstrap_servers = 'kafka:9092', group_id = 'group')
        await cls._kafka_consumer.start()

        # ウェイト情報をkafkaのシグナルに従って読み込み、トレード設定をする非同期タスクを起動する
        asyncio.create_task(_instance._wait_weight_loop_async())

        # 指定された時間間隔で注文を試みる非同期タスクを起動する
        asyncio.create_task(_instance._execute_trades_loop_async())

    def _update_target_weight(self):
        """
        目標ウェイトと設定時間をDBから変数に読み込むメソッド

        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        _sql = f'SELECT datetime, symbol, weight FROM "{self._weight_table_name}" ORDER BY datetime DESC, symbol ASC LIMIT {self._components_num}'
        try:
            _df = TimescaleDBManager.read_sql_query(_sql, self._weight_db_name, dtype = {'datetime': object, 'symbol': str, 'weight': str})
            _df = _df.loc[_df.loc[:, 'datetime'] == _df.iloc[0, _df.columns.get_loc('datetime')], :]
            
            _to_decimal = lambda x: Decimal(x)
            _df['weight'] = _df['weight'].apply(_to_decimal)
        except Exception as e:
            AsyncManager.log_error(f'WeightManager.calc_portfolio_weight() : Cannot read weight from DB. {e}')
            return
        
        self._origin_datetime = _df.iloc[0, _df.columns.get_loc('datetime')].to_pydatetime()
        self._target_datetime = self._origin_datetime + self._rebalance_time

        _df = _df.reset_index(drop = True).set_index('symbol')
        self._target_weight = _df.loc[:, 'weight']
        self._last_step = 0

        AsyncManager.log_info(f'WeightManager._update_target_weight() : Updating weight. origin = {self._origin_datetime} target = {self._target_datetime}\n')
        AsyncManager.log_info(_df)

    async def _wait_weight_loop_async(self):
        """
        目標ウェイトの更新を待ち、更新された値を読み込む無限ループ関数

        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        while True:
            try:
                _msg_dict = await self._kafka_consumer.getmany(timeout_ms=100)
            except OffsetOutOfRangeError as err:
                tps = err.args[0].keys()
                await self._kafka_consumer.seek_to_beginning(*tps)
                continue
            except Exception as e:
                AsyncManager.log_error(f'TradeManager._wait_weight_loop_async() : Kafka consume failed. Exception {e}')
                await asyncio.sleep(1.0)
                continue
            
            if len(_msg_dict) > 0 or self._target_weight is None:
                for k, v in _msg_dict.items():
                    for msg in v:
                        AsyncManager.log_info(f'TradeManager._wait_weight_loop_async() : consumed msg {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}')
                
                # 目標ウェイトを更新
                self._update_target_weight()
            
            await asyncio.sleep(1.0)
    
    def log_symbols_values(self, table_name: str = None, now_datetime: datetime = None, values: pd.Series = None):
        assert table_name is not None
        assert now_datetime is not None
        assert values is not None

        _values = values[values != 0]

        for _symbol in _values.index.values:
            _value = _values.loc[_symbol]
            try:
                _sql = f'insert into "{table_name}" (datetime, symbol, value) values (\'{now_datetime}\', \'{_symbol}\', {_value})'
                TimescaleDBManager.execute_sql(_sql, self.__class__.__name__)
            except Exception as e:
                AsyncManager.log_error(f'TimescaleDBManager.log_symbols_values(table_name = {table_name}, values = {values}) : Insert failed. Exception {e}')
                return False
        return True
    
    def log_order_attempt(self, now_datetime: datetime = None, symbol: str = None, base_amount: object = None, close: object = None, best_bid: object = None, best_bid_qty: object = None, best_ask: float = None, best_ask_qty: float = None):
        _exchange_name = ExchangeManager.get_exchange_name()
        _interval_str = TimebarManager.get_interval_str(TimebarManager._timebar_interval)
        _table_name = f'{_exchange_name}_order_attempt_log_{_interval_str}'

        try:
            _sql = f'insert into "{_table_name}" (datetime, symbol, base_amount, close, best_bid, best_bid_qty, best_ask, best_ask_qty) values (\'{now_datetime}\', \'{symbol}\', {base_amount}, {close}, {best_bid}, {best_bid_qty}, {best_ask}, {best_ask_qty})'
            TimescaleDBManager.execute_sql(_sql, self.__class__.__name__)
        except Exception as e:
            AsyncManager.log_error(f'TradeManager.log_order_attempt(\'{now_datetime}\', \'{symbol}\', {base_amount}, {close}, {best_bid}, {best_bid_qty}, {best_ask}, {best_ask_qty}) : Insert failed. Exception {e}')
            return False
        
        return True

    async def _execute_trades_loop_async(self):
        """
        目標ウェイトへのトレードを一定期間おきに繰り返す無限ループ関数

        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        # 目標ウェイトの初期値を読み込む
        self._update_target_weight()

        # self._action_interval_sec間隔でトレードを試み続けるループ
        while True:
            if self._target_weight is None or self._target_datetime is None:
                await asyncio.sleep(1.0)
                continue
            
            _seconds_in_rebalance_cycle: int = int((datetime.now(timezone.utc) - self._origin_datetime).total_seconds())
            _step_in_rebalance_cycle: int = int(_seconds_in_rebalance_cycle // self._trade_interval.total_seconds())
            
            if _step_in_rebalance_cycle == self._last_step:
                # 最後のステップが現在ステップと同じ場合は、前回処理してからまだstep_sec秒経過していないので何もしない
                await asyncio.sleep(1.0)
                continue
                        
            # ここからリバランス執行処理
            self._last_step = _step_in_rebalance_cycle
            AsyncManager.log_info(f'TradeManager._execute_trades_loop_async() : Entering new step {_step_in_rebalance_cycle} / {self._step_count}')
            
            # 証拠金のUSDTバリューと現在ポジションを取得
            _cw_usdt_balance = ExchangeManager.get_usdt_cw_margin()
            await ExchangeManager.update_position_async()
            _position_df = ExchangeManager.get_position_df()

            if _position_df is None:
                # ポジション情報がまだないので、リバランスは行わない
                AsyncManager.log_warning(f'TradeManager._execute_trades_loop_async() : No position information yet. Skipping')
                await asyncio.sleep(1.0)
                continue

            # 現在のポジションを表示
            ExchangeManager.print_positions()

            # 銘柄ごとの現在のUSDTバリューを計算
            _current_value_series = _position_df.loc[:, 'usdt_value']
            _amount_series = _position_df.loc[:, 'amount']
            _total_abs_usdt_value = _position_df.loc[:, 'abs_usdt_value'].sum()
            _unrealized_pnl_series = _position_df.loc[:, 'unrealized_pnl']
            _total_unrealized_pnl = _unrealized_pnl_series.sum()

            if _total_abs_usdt_value > 0:
                # 現在ウェイトを計算する
                _current_weight = _current_value_series / (_cw_usdt_balance + _total_unrealized_pnl)
            else:
                # 全くポジションがない場合は、現在ウェイトを0とする
                _current_weight = _current_value_series.copy()
                _current_weight.loc[:] = Decimal(0)
            
            _df_weights = pd.concat([_current_weight, self._target_weight], axis = 1).fillna(Decimal(0))
            _df_weights.columns = ['current_weight', 'final_weight']
            _df_weights.loc[:, 'next_weight'] = Decimal(0)
            
            _df_weights.loc[:, 'next_weight'] = _df_weights.loc[:, 'current_weight'] + (_df_weights.loc[:, 'final_weight'] - _df_weights.loc[:, 'current_weight']) / (self._step_count - min(self._step_count - 1, _step_in_rebalance_cycle))

            _current_weight_sum: Decimal = _df_weights['current_weight'].abs().sum()
            _next_weight_sum: Decimal = _df_weights['next_weight'].abs().sum()
            _final_weight_sum: Decimal = _df_weights['final_weight'].abs().sum()
            
            AsyncManager.log_info(f'\n{_df_weights[_df_weights.any(axis = 1)]}\n'\
                                  f'current_weight sum = {_current_weight_sum.quantize(Decimal("0.01"), rounding = ROUND_HALF_UP)}\n'\
                                  f'next_weight sum = {_next_weight_sum.quantize(Decimal("0.01"), rounding = ROUND_HALF_UP)}\n'\
                                  f'final_weight sum = {_final_weight_sum.quantize(Decimal("0.01"), rounding = ROUND_HALF_UP)}')

            # 現在ウェイトやポジションなどをログテーブルに記入する
            _now_datetime = datetime.now(tz = timezone.utc)
            _exchange_name = ExchangeManager.get_exchange_name()
            _interval_str = TimebarManager.get_interval_str(TimebarManager._timebar_interval)

            _table_name = f'{_exchange_name}_current_weight_{_interval_str}'
            self.log_symbols_values(_table_name, _now_datetime, _current_weight)

            _table_name = f'{_exchange_name}_target_weight_{_interval_str}'
            self.log_symbols_values(_table_name, _now_datetime, self._target_weight)

            _table_name = f'{_exchange_name}_current_value_{_interval_str}'
            self.log_symbols_values(_table_name, _now_datetime, _current_value_series)

            _table_name = f'{_exchange_name}_current_upnl_{_interval_str}'
            _unrealized_pnl_series['cw_usdt_balance'] = _cw_usdt_balance
            self.log_symbols_values(_table_name, _now_datetime, _unrealized_pnl_series)

            if self._step_count <= _step_in_rebalance_cycle:
                # すでに全ステップを超えるステップ数に到達している場合は、現ポジションを維持したまま次のリバランス開始を待つ
                AsyncManager.log_info(f'   Rebalance cycle already completed. Skipping this step. {_step_in_rebalance_cycle} / {self._step_count}')  
                await asyncio.sleep(1.0)
                continue
                    
            # 目標バリューからの差、および注文すべきロットを計算する
            _target_value_series = _df_weights.loc[:, 'next_weight'] * (_cw_usdt_balance + _total_unrealized_pnl)
            _diff_value_series = (_target_value_series - _current_value_series)

            # 注文を出す
            _orders = []
            for k in _diff_value_series.keys():
                _order_value = _diff_value_series[k]
                if _order_value != 0 and k != 'USDTUSDT':
                    _bookticker = self._datastore.bookticker.find({'s': k})
                    if len(_bookticker) == 0:
                        AsyncManager.log_warning(f'    Skipping no book info : {k.replace("USDT", "")}')
                        continue

                    _ticker = self._datastore.ticker.find({'s': k})
                    if len(_ticker) == 0:
                        AsyncManager.log_warning(f'    Skipping no close_price : {k.replace("USDT", "")}')
                        continue

                    
                    # 仮のオーダー量を計算する
                    _stepsize = ExchangeManager.get_trade_stepsize(k)
                    _close = Decimal(_ticker[0]['c'])
                    _order_lot = _order_value / _close // _stepsize * _stepsize
                    _order_lot_value = _order_lot * _close

                    if abs(_target_value_series[k]) == Decimal(0):
                        # 次のターゲットバリューが0の場合は、現在ポジションの反対を売買量とする
                        _order_lot = -_amount_series[k]
                        _order_lot_value = _order_lot * _close
                        
                    if abs(_order_lot_value) < Decimal(11) or abs(_order_lot) < _stepsize:
                        AsyncManager.log_info(f'    Skipping too small order : {_order_lot} {k.replace("USDT", "")} (close {_close} total {abs(_order_lot_value)} USDT value, min lot size = {ExchangeManager.get_trade_minlot(k)})')
                        continue
                    
                    AsyncManager.log_info(f'    Adding order :  {_order_lot} {k.replace("USDT", "")} (close {_close} total {abs(_order_lot_value)} USDT value, min lot size = {ExchangeManager.get_trade_minlot(k)})')
                    
                    # オーダー配列にオーダーを追加
                    _orders.append({'symbol': k, 'side': 'BUY' if _order_lot > 0 else 'SELL', 'type': 'MARKET', 'quantity': abs(_order_lot)})

                    # オーダーログを記録
                    self.log_order_attempt(_now_datetime, k, _order_lot, _close, Decimal(_bookticker[0]['b']), Decimal(_bookticker[0]['B']), Decimal(_bookticker[0]['a']), Decimal(_bookticker[0]['A']))

            # オーダーを並列実行する
            _order_tasks = [self._execute_order(_order) for _order in _orders]
            _responses = await asyncio.gather(*_order_tasks)

            AsyncManager.log_info(f'   All orders completed')
            await asyncio.sleep(1.0)
    
    async def _execute_order(self, order: dict):
        # リトライカウンターを初期化
        _retry = 0

        while True:
            _api_use_result = ExchangeManager.use_api_weight(1)
            if _api_use_result == False:
                AsyncManager.log_warning(f'   {order["symbol"]} Retry. use_api_weight failed.')
                await asyncio.sleep(1.0)
                continue

            if _retry > 3:
                AsyncManager.log_warning(f'   More than {_retry - 1} retry. Abort {order}')
                break

            AsyncManager.log_info(f'   {order["symbol"]} order attempt (retry: {_retry}) {order}')
            try:
                _client = PyBottersManager.get_client()
                _result = await _client.post('/fapi/v1/order', data = order.copy())
                if _result.status != 200:
                    _text = await _result.text()
                    AsyncManager.log_warning(f'   {order["symbol"]} order retry. Status {_result.status} Text {_text}')
                    await asyncio.sleep(1.0)
                    _retry = _retry + 1
                    continue

                return _result
            except BaseException as e:
                # 例外は0.1秒待ってリトライ
                AsyncManager.log_warning(f'   {order["symbol"]} Retry. Exception {e}')
                await asyncio.sleep(1.0)
                _retry = _retry + 1
                continue

if __name__ == "__main__":
    # 一定間隔でトレードをしながら、ウェイトの更新をトリガーに目標ウェイトを更新し続けるプログラム
    import argparse
    from os import environ
    from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis, wm_config, tm_config
    from logging import Logger, getLogger, basicConfig, handlers
    import logging
    from rich.logging import RichHandler

    async def async_task():
        # コマンドライン引数の取得
        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--testnet', help = 'Download mainnet timebar', action = 'store_true')
        args = parser.parse_args()

        # AsyncManagerの初期化
        _filehandler = handlers.TimedRotatingFileHandler('trade_manager.log', when = 'D', encoding = 'utf-8', utc = True)
        _filehandler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"))
        _richhandler = RichHandler(rich_tracebacks = True)
        _richhandler.setFormatter(logging.Formatter('%(message)s'))

        basicConfig(level = logging.INFO, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler,_filehandler])
        _logger: Logger = getLogger('rich')
        
        AsyncManager.set_logger(_logger)

        # TimebarManagerの初期化前に、TimescaleDBManagerの初期化が必要
        TimescaleDBManager(pg_config)

        # TimebarManagerの初期化前に、PyBottersManagerの初期化が必要
        # コマンドラインパラメータから、マネージャー初期化用パラメータを取得
        if args.testnet == True:
            _pybotters_params = binance_testnet_config.copy()
            _exchange_params = binance_testnet_config.copy()
        else:
            _pybotters_params = binance_config.copy()
            _exchange_params = binance_config.copy()

        # PyBottersの初期化
        _pybotters_params['apis'] = pybotters_apis.copy()
        PyBottersManager(_pybotters_params)

        # タイムバーをダウンロードするだけなら、run_asyncを読んでWebsocket APIからポジション情報等をダウンロードする必要はない
        _exchange_config = binance_testnet_config.copy()
        ExchangeManager(_exchange_config)
        await ExchangeManager.run_async()

        # TimebarManagerの初期化
        _timebar_params = {
            'timebar_interval': timedelta(minutes = 5)
        }
        TimebarManager(_timebar_params)

        # WeightManagerの初期化
        wm_config['timebar_table_name'] = TimebarManager.get_table_name()
        WeightManager(wm_config)

        tm_config['ws_baseurl'] = _exchange_config['ws_baseurl']
        tm_config['weight_table_name'] = WeightManager.get_table_name()
        TradeManager(tm_config)
        await TradeManager.run_async()

        while True:
            await asyncio.sleep(60.0)

    try:
        asyncio.run(async_task())
    except KeyboardInterrupt:
        pass
