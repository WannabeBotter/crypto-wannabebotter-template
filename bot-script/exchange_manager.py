import asyncio
from decimal import Decimal
from datetime import datetime, timezone
from logging import Logger

import pandas as pd

import pybotters

from async_manager import AsyncManager
from timescaledb_manager import TimescaleDBManager
from pybotters_manager import PyBottersManager

class ExchangeManager(AsyncManager):
    # グローバル共有のインスタンスを保持するクラス変数
    _instance: object = None

    # オーダーイベントの辞書キーとDB内のカラム名の対象用の辞書
    _db_columns_dict = {
        'dt': ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
        's': ('symbol', 'TEXT', 'NOT NULL'),
        'c': ('client_order_id', 'TEXT', 'NOT NULL'),
        'S': ('side', 'TEXT', 'NOT NULL'),
        'o': ('order_type', 'TEXT', 'NOT NULL'),
        'f': ('time_in_force', 'TEXT' ,'NOT NULL'),
        'q': ('orig_quantity', 'NUMERIC', 'NOT NULL'),
        'p': ('orig_price', 'NUMERIC', 'NOT NULL'),
        'ap': ('ave_price', 'NUMERIC', 'NOT NULL'),
        'sp': ('stop_price', 'NUMERIC', 'NOT NULL'),
        'x': ('exec_type', 'TEXT', 'NOT NULL'),
        'X': ('order_status', 'TEXT', 'NOT NULL'),
        'i': ('order_id', 'BIGINT', 'NOT NULL'),
        'l': ('order_last_fill_quantity', 'NUMERIC', 'NOT NULL'),
        'z': ('order_fill_total_quantity', 'NUMERIC', 'NOT NULL'),
        'L': ('last_fill_price', 'NUMERIC', 'NOT NULL'),
        'N': ('commission_symbol', 'TEXT', 'DEFAULT \'NONE\''),
        'n': ('commission', 'NUMERIC', 'DEFAULT 0'),
        'T': ('order_trade_time', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
        't': ('trade_id', 'BIGINT', 'NOT NULL'),
        'b': ('bid_notional', 'NUMERIC', 'NOT NULL'),
        'a': ('ask_notional', 'NUMERIC', 'NOT NULL'),
        'm': ('if_maker', 'BOOLEAN', 'NOT NULL'),
        'R': ('if_reduce_only', 'BOOLEAN', 'NOT NULL'),
        'wt': ('stop_price_type', 'TEXT', 'NOT NULL'),
        'ot': ('orig_order_type', 'TEXT', 'NOT NULL'),
        'ps': ('position_side', 'TEXT', 'NOT NULL'),
        'cp': ('cond_close_all', 'BOOLEAN', 'NOT NULL'),
        'AP': ('activation_price', 'NUMERIC', 'DEFAULT 0'),
        'cr': ('callback_rate', 'NUMERIC', 'DEFAULT 0'),
        'rp': ('realized_profit', 'NUMERIC', 'NOT NULL')
    }

    def __init__(self, params: dict = None):
        """
        ExchangeManagerコンストラクタ
        
        Parameters
        ----------
        params : dict
            (必須) 初期化パラメータが入った辞書
        params['exchange_name'] : str
            (必須) DBアクセス時に利用する取引所名
        params['client'] : pybotters.Client
            (必須) PyBotters.Clientのインスタンス
        params['ws_baseurl'] : str
            (必須) WebsocketAPIのベースURL

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert params['exchange_name'] is not None
        assert params['ws_baseurl'] is not None

        if ExchangeManager._instance is None:
            self._exchange_name: str = params['exchange_name']
            self._ws_baseurl: str = params['ws_baseurl']

            # 取引所情報の辞書
            self._exchange_info: dict = None

            # 全USDTパーペチュアル銘柄を保持するset
            self._all_symbols: set = None

            # トレード時の最小ロット数・最大ロット数・ロット単位を記録する辞書
            self._maxlot_series: pd.Series = None
            self._minlot_series: pd.Series = None
            self._stepsize_series: pd.Series = None

            # 最新のマーク価格とクローズ価格を保持するSeries
            self._latest_close_series = None
            self._latest_mark_series = None

            # APIのウェイト管理用
            self._api_weight = 0
            self._api_last_reset_idx = 0

            # データストアの初期化
            self._datastore = pybotters.BinanceDataStore()

            # データベースの初期化とインスタンスの保存
            ExchangeManager._instance = self
            ExchangeManager.init_database()

    @classmethod
    async def run_async(cls) -> None:
        """
        ExchangeManagerの非同期タスクループ起動用メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert ExchangeManager._instance is not None

        _instance: ExchangeManager = ExchangeManager._instance
        _client: pybotters.Client = PyBottersManager.get_client()

        assert _client is not None

        # 取引所情報を取得する
        await ExchangeManager.update_exchangeinfo_async()

        # データストアを初期化する
        await _instance._datastore.initialize(
            _client.get('/fapi/v1/openOrders'),
            _client.get('/fapi/v2/positionRisk'),
            _client.get('/fapi/v2/balance'),
            _client.post('/fapi/v1/listenKey')
        )

        # アカウントのバランス・ポジション変化にsubscribeする
        asyncio.create_task(_client.ws_connect(f'{_instance._ws_baseurl}/ws/{_instance._datastore.listenkey}', hdlr_json = _instance._datastore.onmessage, heartbeat = 10.0))
        asyncio.create_task(_client.ws_connect(f'{_instance._ws_baseurl}/ws/!markPrice@arr@1s', hdlr_json = _instance._datastore.onmessage, heartbeat = 10.0))

        # オーダー情報をwebsocketから受け取りログをDBに保存する非同期タスクを起動する
        asyncio.create_task(ExchangeManager._order_update_loop_async())

    @classmethod
    async def update_exchangeinfo_async(cls) -> None:
        """
        クラス内で保持している取引所情報を最新の状態に更新するメソッド
        
        Parameters
        ----------
        なし

        Returns
        ----------
        なし
        """
        assert ExchangeManager._instance is not None

        _instance: ExchangeManager = ExchangeManager._instance
        _client: pybotters.Client = PyBottersManager.get_client()

        assert _client is not None

        # 取引所情報をREST APIから取得する。リトライ回数は無限
        while True:
            try:
                _r = await _client.get('/fapi/v1/exchangeInfo')
                if _r.status == 200:
                    break
                else:
                    # 200以外は1秒待ってリトライ
                    AsyncManager.log_warning(f'ExchangeManager.update_exchangeinfo_async() : Retry. Non 200 status {_r.status}')
                    await asyncio.sleep(1.0)
            except BaseException as e:
                AsyncManager.log_warning(f'ExchangeManager.update_exchangeinfo_async() : Retry. Exception {e}')
                await asyncio.sleep(1.0)

        _instance._exchange_info = await _r.json()

        # USDT建ての無期限先物銘柄のリストを更新
        _instance._all_symbols = set()
        for symbol in _instance._exchange_info['symbols']:
            if '_' not in symbol['symbol'] and 'USDT' in symbol['symbol']:
                _instance._all_symbols.add(symbol['symbol'])
        _instance._all_symbols = sorted(_instance._all_symbols)

        # 取引所のAPI呼び出しリミットを更新
        _instance._api_weight_reset_value = _instance._exchange_info['rateLimits'][0]['limit']
        _instance._api_weight_reset_interval_sec = _instance._exchange_info['rateLimits'][0]['intervalNum'] * 60

        # 銘柄ごとの最小注文量、最大注文量、注文ステップを更新
        _instance._maxlot_series = pd.Series(dtype = object)
        _instance._minlot_series = pd.Series(dtype = object)
        _instance._stepsize_series = pd.Series(dtype = object)

        _instance._maxlot_series['USDTUSDT'] = Decimal(10000000.0)
        _instance._minlot_series['USDTUSDT'] = Decimal(0.01)
        _instance._stepsize_series['USDTUSDT'] = Decimal(0.01)

        _instance._symbol_lotinfo = {}
        for _symbol in _instance._exchange_info['symbols']:
            if _symbol['symbol'] in _instance._all_symbols:
                _symbol_str = _symbol['symbol']
                for _filter in _symbol['filters']:
                    if _filter['filterType'] == 'MARKET_LOT_SIZE':
                        _instance._maxlot_series[_symbol_str] = Decimal(_filter['maxQty'])
                        _instance._minlot_series[_symbol_str] = Decimal(_filter['minQty'])
                        _instance._stepsize_series[_symbol_str] = Decimal(_filter['stepSize'])
                        break
    
    @classmethod
    async def update_position_async(cls) -> None:
        """
        PyBottersのデータストア内で保持しているポジション情報を最新の状態に更新するメソッド\n
        PyBottersのmodels/binance.pyのPosition._onresponseに "up": item["unRealizedProfit"]を追加するか、Develop版を利用すること
        
        Parameters
        ----------
        なし

        Returns
        ----------
        なし
        """
        assert ExchangeManager._instance is not None

        _instance: ExchangeManager = ExchangeManager._instance

        while True:
            try:
                if ExchangeManager.use_api_weight(5) == True:
                    _client = PyBottersManager.get_client()
                    _r = await _client.get('/fapi/v2/positionRisk')
                    if _r.status == 200:
                        _data = await _r.json()
                        _instance._datastore.position._onresponse(_data)
                        break
                    else:
                        # 200以外は1秒待ってリトライ
                        AsyncManager.log_warning(f'ExchangeManager.update_position_async() : Retry. Non 200 status {_r.status}')
                        await asyncio.sleep(1.0)
                else:
                    # APIを呼び出しすぎている
                    AsyncManager.log_warning(f'ExchangeManager.update_position_async() : Retry. API weight shortage.')
                    await asyncio.sleep(1.0)
            except BaseException as e:
                AsyncManager.log_warning(f'ExchangeManager.update_position_async() : Retry. Exception {e}')
                await asyncio.sleep(1.0)
    
    @classmethod
    def use_api_weight(cls, weight: int) -> bool:        
        """
        API利用のために必要な残ウェイトが残っているか否かを判定する関数
        
        Parameters
        ----------
        weight : int, 必須
            利用したいウェイト数
        timebar_list : list, 必須
            取得するタイムバーの最大数。デフォルト値=1
        
        Returns
        ----------
        True
            残ウェイトが十分でAPIコールができる
        False
            残ウェイトが足りずAPIコールをしてはならない
        """
        assert weight > 0
        assert ExchangeManager._instance is not None

        _instance: ExchangeManager = ExchangeManager._instance

        _now_idx = int(datetime.now(tz = timezone.utc).timestamp()) // _instance._api_weight_reset_interval_sec
        if _now_idx > _instance._api_last_reset_idx:
            _instance._api_last_reset_idx = _now_idx
            _instance._api_weight = _instance._api_weight_reset_value
        if _instance._api_weight < weight:
            return False
        _instance._api_weight -= weight
        return True

    @classmethod
    async def _order_update_loop_async(cls):
        """
        オーダー情報をWebsocketから受け取り、DBに保存する非同期タスク
        
        Parameters
        ----------
        なし

        Returns
        ----------
        なし
        """
        assert ExchangeManager._instance is not None

        _instance: ExchangeManager = ExchangeManager._instance
        _table_name: str = ExchangeManager.get_table_name()

        while True:
            _events = await _instance._datastore.order.wait()
            AsyncManager.log_info(f'ExchangeManager._order_update_loop_async() : Pybotters datastore event received\n{_events}')
            for _event in _events:
                ExchangeManager.log_order_update(_event)
    
    @classmethod
    def get_exchange_name(cls) -> str:
        """
        DBで利用する取引所名を取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        テーブル名 : str
        """
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        return _instance._exchange_name.lower()

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
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        return f'{_instance._exchange_name}_order_log'.lower()
    
    @classmethod
    def get_cw_margin(cls) -> Decimal:
        """
        USDTの証拠金残額を取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        Decimal
            USDTの証拠金残額
        """
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        cw_balance = Decimal(0)

        for _symbol in ['USDT', 'USDC', 'BUSD']:
            _balance = _instance._datastore.balance.find({'a': _symbol})

            if _balance is not None and len(_balance) > 0:
                cw_balance += Decimal(_balance[0]['cw'])

        return cw_balance
    
    @classmethod
    def get_trade_stepsize(cls, symbol: str = None):
        assert ExchangeManager._instance is not None
        assert symbol is not None

        return ExchangeManager._instance._stepsize_series[symbol]

    @classmethod
    def get_trade_minlot(cls, symbol: str = None):
        assert ExchangeManager._instance is not None
        assert symbol is not None

        return ExchangeManager._instance._minlot_series[symbol]

    @classmethod
    def get_all_symbols(cls) -> set:
        """
        現在対応している全銘柄の一覧を取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        set
            全銘柄名を含んだset (銘柄名は全て大文字でbaseアセットとquoteアセットの区切り文字はない)
        """
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        return _instance._all_symbols.copy()


    @classmethod
    def _get_latest_markprice(cls) -> pd.Series:
        """
        最新のマーク価格を全銘柄分取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        Series
            全銘柄分の最新のマーク価格
        """
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        _mark_list = _instance._datastore.markprice.find()

        if _instance._latest_mark_series is None:
            _instance._latest_mark_series = pd.Series(dtype = object)
            _instance._latest_mark_series['USDTUSDT'] = Decimal(1.0)
            _instance._latest_mark_series.name = 'mark_price'

        for _markprice in _mark_list:
            _symbol_str = _markprice['s']
            if '_' not in _symbol_str and 'USDT' in _symbol_str:
                _instance._latest_mark_series[_symbol_str] = Decimal(_markprice['p'])
                if _symbol_str not in _instance._all_symbols:
                    _instance._all_symbols.add(_symbol_str)
        
        return _instance._latest_mark_series.copy()

    @classmethod
    def _get_latest_close(cls) -> pd.Series:
        """
        最新のクローズ価格を全銘柄分取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        pd.Series
            全銘柄分の最新のクローズ価格
        """
        assert ExchangeManager._instance is not None
        
        _instance: ExchangeManager = ExchangeManager._instance

        _close_list = _instance._datastore.ticker.find()

        if _instance._latest_close_series is None:
            _instance._latest_close_series = pd.Series(dtype = object)
            _instance._latest_close_series['USDTUSDT'] = Decimal(1.0)
            _instance._latest_close_series.name = 'close'

        for _close in _close_list:
            _symbol_str = _close['s']
            if '_' not in _symbol_str and 'USDT' in _symbol_str:
                _instance._latest_close_series[_symbol_str] = Decimal(_close['c'])
                if _symbol_str not in _instance._all_symbols:
                    _instance._all_symbols.add(_symbol_str)

        return _instance._latest_close_series.copy()

    @classmethod
    def get_position_df(cls) -> pd.DataFrame:
        """
        最新のポジション情報を取得する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        pd.DataFrame['amount'] : Decimal
            ポジションの数量
        pd.DataFrame['entry_price'] : Decimal
            平均エントリー価格
        pd.DataFrame['mark_price'] : Decimal
            最新のマーク価格
        pd.DataFrame['abs_usdt_value'] : Decimal
            ポジションのUSDT建て価格の絶対値
        pd.DataFrame['unrealized_pnl'] : Decimal
            ポジションのUSDT建て未実現損益        
        """
        assert ExchangeManager._instance is not None
        _instance: ExchangeManager = ExchangeManager._instance

        _position_list = _instance._datastore.position.find()
        _close_series = _instance._get_latest_close()

        _close_index_set = set(_close_series.index.values)

        if len(_position_list) == 0:
            AsyncManager.log_warning(f'ExchangeManager.get_position() : Position data is not avalable yet. Abort.')
            return None
        
        _position_dict = {}
        for _position in _position_list:
            _symbol_str = _position['s']
            if _symbol_str in _instance._all_symbols:
                _position_amount = Decimal(_position['pa']) 
                _entry_price = Decimal(_position['ep'])
                _unrealized_profit = Decimal(_position['up'])

                if _position_amount == Decimal(0):
                    _mark_price = _entry_price
                else:
                    _mark_price = _entry_price + _unrealized_profit / _position_amount
                
                _position_dict[_symbol_str] = {
                    'amount': _position_amount,
                    'entry_price': _entry_price,
                    'mark_price': _mark_price,
                    'usdt_value': _position_amount * _mark_price,
                    'abs_usdt_value': abs(_position_amount) * _mark_price,
                    'unrealized_pnl': _unrealized_profit
                }
        
        position_df = pd.DataFrame.from_dict(_position_dict, orient = 'index')
        position_df = position_df.sort_values('abs_usdt_value', ascending = False)

        return position_df

    @classmethod
    def print_positions(cls) -> None:
        """
        最新のポジション情報をログ出力する関数
        
        Parameters
        ----------
        なし
        
        Returns
        ----------
        なし。
        """
        assert ExchangeManager._instance is not None

        _position_df = ExchangeManager.get_position_df()
        if _position_df is None:
            AsyncManager.log_warning(f'ExchangeManager.print_position() : Position data is not avalable yet. Abort.')
            return

        _cw_usdt_balance = ExchangeManager.get_cw_margin()
        _total_usdt_value = _position_df.loc[:, "usdt_value"].sum()
        _total_abs_usdt_value = _position_df.loc[:, 'abs_usdt_value'].sum()
        _total_unrealized_pnl = _position_df.loc[:, 'unrealized_pnl'].sum()

        AsyncManager.log_info(f'\nExchangeManager.print_position()\n{_position_df[_position_df["amount"] != 0]}\nPos value = {_total_usdt_value}\nPos ABS value = {_total_abs_usdt_value}\nUnrealized PnL = {_total_unrealized_pnl}\nMargin balance = {_cw_usdt_balance + _total_unrealized_pnl}')
    
    @classmethod
    def init_database(cls, force = False):
        """
        オーダー情報用のDBとテーブルを初期化する
        
        Parameters
        ----------
        force : bool
            強制的にテーブルを初期化するか否か (デフォルト値はfalse)

        Returns
        ----------
        なし。失敗した場合は例外をRaiseする
        """
        assert ExchangeManager._instance is not None
        _instance: ExchangeManager = ExchangeManager._instance

        _table_name = ExchangeManager.get_table_name()
        
        try:
            # このマネージャー専用のデータベースの作成を試みる。すでにある場合はそのまま処理を続行する
            TimescaleDBManager.init_database('ExchangeManager')

            # テーブルが存在しているか確認する
            _df = TimescaleDBManager.read_sql_query(f"select * from information_schema.tables where table_name='{_table_name}'", ExchangeManager.__name__)
        except Exception as e:
            AsyncManager.log_error(f'ExchangeManager._init_database(database_name = {ExchangeManager.__name__}, table_name = {_table_name}) : Table initialization failed. Exception {e}')
            raise(e)
        
        if len(_df.index) > 0 and force == False:
            return
                
        # テーブルそのものがないケース
        _columns_str_list = [f'{v[0]} {v[1]} {v[2]}' for k, v in _instance._db_columns_dict.items()]
        _columns_str = ', '.join(_columns_str_list)
        
        # 目標ウェイト記録テーブルを作成
        _sql = (f'DROP TABLE IF EXISTS "{_table_name}" CASCADE;'
                f' CREATE TABLE IF NOT EXISTS "{_table_name}" ({_columns_str});'
                f' CREATE INDEX ON "{_table_name}" (datetime DESC);'
                f" SELECT create_hypertable ('{_table_name}', 'datetime');")
        
        try:
            # テーブルの削除と再作成を試みる            
            TimescaleDBManager.execute_sql(_sql, ExchangeManager.__name__)
        except Exception as e:
            AsyncManager.log_error(f'ExchangeManager._init_database(database_name = {ExchangeManager.__name__}, table_name = {_table_name}) : Create table failed. Exception {e}')
            raise(e)

    @classmethod
    def log_order(cls, order: dict = None):
        """
        DB上にオーダー情報をログとして残す関数
        
        Parameters
        ----------
        order : dict
            (必須) 記録するオーダーそのもの

        Returns
        ----------
        なし。失敗した場合は例外をRaiseする。
        """
        assert ExchangeManager._instance is not None
        assert order is not None
        _instance: ExchangeManager = ExchangeManager._instance

        _sql_dict = {}
        _table_name: str = ExchangeManager.get_table_name()

        for k, v in order.items():
            if k not in _instance._db_columns_dict:
                continue
            
            _type = _instance._db_columns_dict[k][1]
            if _type == 'NUMERIC':
                _sql_dict[_instance._db_columns_dict[k][0]] = Decimal(v)
            elif _type == 'BIGINT':
                _sql_dict[_instance._db_columns_dict[k][0]] = int(v)
            elif _type == 'TIMESTAMP':
                _sql_dict[_instance._db_columns_dict[k][0]] = f'\'{datetime.fromtimestamp(v / 1000, tz = timezone.utc)}\''
            elif _type == 'BOOLEAN':
                _sql_dict[_instance._db_columns_dict[k][0]] = v
            else:
                _sql_dict[_instance._db_columns_dict[k][0]] = f'\'{v.lower()}\''
            
        _sql_dict['datetime'] = _sql_dict['order_trade_time']

        _columns_list = [f'{_column.lower()}' for _column in _sql_dict.keys()]
        _columns_str = ', '.join(_columns_list)
        _values_list = [f'{_value}' for _value in _sql_dict.values()]
        _values_str = ', '.join(_values_list)

        _sql = f'insert into "{_table_name}" ({_columns_str}) values ({_values_str})'
        
        try:
            TimescaleDBManager.execute_sql(_sql)
        except Exception as e:
            AsyncManager.log_error(f'TimescaleDBManager.log_order_update(table_name = {_table_name}, order = {order}) : Insert failed. Exception {e}')
            raise(e)

    @classmethod
    def log_order_update(cls, order: dict = None):
        """
        DB上のオーダー情報を追加する
        
        Parameters
        ----------
        order : dict
            (必須) 記録するオーダーそのもの

        Returns
        ----------
        True
            追加に成功した
        False
            追加に失敗した
        """
        assert order is not None

        _sql_dict = {}
        _db_columns_dict = ExchangeManager._db_columns_dict
        _table_name = ExchangeManager.get_table_name()

        for k, v in order.items():
            if k not in _db_columns_dict:
                continue
            
            _type = _db_columns_dict[k][1]
            if _type == 'NUMERIC':
                _sql_dict[_db_columns_dict[k][0]] = Decimal(v)
            elif _type == 'BIGINT':
                _sql_dict[_db_columns_dict[k][0]] = int(v)
            elif _type == 'TIMESTAMP':
                _sql_dict[_db_columns_dict[k][0]] = f'\'{datetime.fromtimestamp(v / 1000, tz = timezone.utc)}\''
            elif _type == 'BOOLEAN':
                _sql_dict[_db_columns_dict[k][0]] = v
            else:
                _sql_dict[_db_columns_dict[k][0]] = f'\'{v.lower()}\''
            
        _sql_dict['datetime'] = _sql_dict['order_trade_time']

        _columns_list = [f'{_column.lower()}' for _column in _sql_dict.keys()]
        _columns_str = ', '.join(_columns_list)
        _values_list = [f'{_value}' for _value in _sql_dict.values()]
        _values_str = ', '.join(_values_list)

        _sql = f'insert into "{_table_name}" ({_columns_str}) values ({_values_str})'
        
        try:
            TimescaleDBManager.execute_sql(_sql, cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'TimescaleDBManager.log_order_update(table_name = {_table_name}, order = {order}) : Insert failed. Exception {e}')
            return False
        
        return True


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
        _pybotters_params = binance_testnet_config.copy()
        _pybotters_params['apis'] = pybotters_apis.copy()
        PyBottersManager(_pybotters_params)

        # タイムバーをダウンロードするだけなら、run_asyncを読んでWebsocket APIからポジション情報等をダウンロードする必要はない
        _exchange_config = binance_testnet_config.copy()
        ExchangeManager(_exchange_config)
        await ExchangeManager.run_async()

        while True:
            await asyncio.sleep(60.0)

    try:
        asyncio.run(async_task())
    except KeyboardInterrupt:
        pass
