import gc
import asyncio
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import pandas as pd
import pybotters
from aiokafka import AIOKafkaProducer

from async_manager import AsyncManager
from timescaledb_manager import TimescaleDBManager
from exchange_manager import ExchangeManager
from pybotters_manager import PyBottersManager

class TimebarManager(AsyncManager):
    # グローバル共有のインスタンスを保持するクラス変数
    _instance: object = None

    # タイムバー間隔を保持するクラス変数
    _timebar_interval = None

    # オーダーイベントの辞書キーとDB内のカラム名の対象用の辞書
    _db_columns_dict = {
        0: ('datetime', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),
        6: ('datetime_to', 'TIMESTAMP', 'WITH TIME ZONE NOT NULL'),

        200: ('symbol', 'TEXT', 'NOT NULL'),

        1: ('open', 'NUMERIC', 'NOT NULL'),
        2: ('high', 'NUMERIC', 'NOT NULL'),
        3: ('low', 'NUMERIC', 'NOT NULL'),
        4: ('close', 'NUMERIC' ,'NOT NULL'),
        
        # 100 ~ 199のキーはマーク価格APIを呼び出した際に利用する。マーク価格APIから返ってきた添え字に100を足してこの辞書を参照すると対応するカラム情報が取得できる
        101: ('mark_open', 'NUMERIC', 'NOT NULL'),
        102: ('mark_high', 'NUMERIC', 'NOT NULL'),
        103: ('mark_low', 'NUMERIC', 'NOT NULL'),
        104: ('mark_close', 'NUMERIC' ,'NOT NULL'),

        8: ('trade_count', 'NUMERIC', 'NOT NULL'),

        5: ('base_volume', 'NUMERIC', 'NOT NULL'),
        7: ('quote_volume', 'NUMERIC', 'NOT NULL'), # USDT建て
        9: ('quote_buy_volume', 'NUMERIC', 'NOT NULL'), # USDT建て
        10: ('base_buy_volume', 'NUMERIC', 'NOT NULL'),
        
        # 200以上のキーは実際にBinance APIから送られてくることはなく、こちらで計算してDBに入れる値
        201: ('quote_lqd_volume', 'NUMERIC', 'NOT NULL'), # USDT建て
        202: ('quote_lqd_buy_volume', 'NUMERIC', 'NOT NULL'), # USDT建て

        203: ('quote_volume_cumsum', 'NUMERIC', 'NOT NULL'), # USDT建て
        204: ('quote_buy_volume_cumsum', 'NUMERIC', 'NOT NULL') # USDT建て
    }

    # 秒をTimescaleDBとBinance APIで利用できるタイムバー間隔に変換するためのdict
    _timebar_interval_dict = {
        60: '1m',
        180: '3m',
        300: '5m',
        900: '15m',
        1800: '30m',
        3600: '1h',
        7200: '2h',
        14400: '4h',
        28800: '8h',
        43200: '12h',
        86400: '1d',
        259200: '3d',
        604800: '7d',
        2419200: '1M', # 1 month = 28 days
        2505600: '1M', # 1 month = 29 days
        2592000: '1M', # 1 month = 30 days
        2678400: '1M', # 1 month = 31 days
    }
        
    @classmethod
    def _convert_timebar_list_to_timescaledb_dict(self, timebar_list: list = None, markprice = False) -> dict:
        """
        Binance REST APIで得られたリスト形式のタイムバーをTimeScaleDB内のカラム定義に沿った辞書型形式に変換する関数

        Parameters
        ----------
        timebar_list : list
            (必須) 変換元のリスト形式のタイムバー
        markprice : boolearn
            変換対象がマーク価格か否か

        Returns
        -------
        dict
            TimescaleDBUtilが利用する辞書型形式のタイムバー
        """
        assert timebar_list is not None, "timebar_list must not None"
        assert len(timebar_list) == 12, "Timebar list length isn't 12"

        if markprice == True:
            return {
                'datetime': datetime.fromtimestamp(timebar_list[0] / 1000, tz=timezone.utc),
                'mark_open': Decimal(timebar_list[1]),
                'mark_high': Decimal(timebar_list[2]),
                'mark_low': Decimal(timebar_list[3]),
                'mark_close': Decimal(timebar_list[4]),
            }

        return {
            'datetime': datetime.fromtimestamp(timebar_list[0] / 1000, tz=timezone.utc),
            'datetime_to': datetime.fromtimestamp(timebar_list[6] / 1000, tz=timezone.utc),
            'open': Decimal(timebar_list[1]),
            'high': Decimal(timebar_list[2]),
            'low': Decimal(timebar_list[3]),
            'close': Decimal(timebar_list[4]),
            'base_volume': Decimal(timebar_list[5]),
            'quote_volume': Decimal(timebar_list[7]),
            'trade_count': Decimal(timebar_list[8]),
            'base_buy_volume': Decimal(timebar_list[9]),
            'quote_buy_volume': Decimal(timebar_list[10])
        }

    def __init__(self, params: dict = None):
        """
        TimebarManagerコンストラクタ
        
        Parameters
        ----------
        params['timebar_interval'] : timedelta
            (必須) このTimebarManagerで利用するタイムバーの間隔

        Returns
        -------
        なし
        """
        assert params['timebar_interval'] is not None

        TimebarManager._timebar_interval: timedelta = params['timebar_interval']

    @classmethod
    async def init_async(cls, params: dict = None) -> object:
        """
        TimebarManagerのインスタンスを作成し初期化する関数
        
        Parameters
        ----------
        params['timebar_interval'] : timedelta
            (必須) このTimebarManagerで利用するタイムバーの間隔

        Returns
        -------
        なし。初期化に失敗した場合は例外をRaiseする
        """
        assert params['timebar_interval'] is not None

        if TimebarManager._instance is not None:
            return
        else:
            TimebarManager._instance: TimebarManager = TimebarManager(params)
            TimebarManager.init_database()

            TimebarManager._kafka_producer = AIOKafkaProducer(bootstrap_servers = 'kafka:9092')
            await TimebarManager._kafka_producer.start()

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
        assert TimebarManager._instance is not None

        _instance: TimebarManager = TimebarManager._instance

        # オーダー情報をwebsocketから受け取りログをDBに保存する非同期タスクを起動する
        asyncio.create_task(_instance._update_all_klines_loop_async())

    @classmethod
    def init_database(cls, force = False):
        """
        タイムバー情報用のDBとテーブルを初期化する
        
        Parameters
        ----------
        force : bool
            強制的にテーブルを初期化するか否か (デフォルト値はfalse)

        Returns
        ----------
        なし。失敗した場合は例外をRaiseする
        """
        assert TimebarManager._instance is not None

        _instance: TimebarManager = TimebarManager._instance
        _table_name: str = TimebarManager.get_table_name()

        try:
            # このマネージャー専用のデータベースの作成を試みる。すでにある場合はそのまま処理を続行する
            TimescaleDBManager.init_database(cls.__name__)

            # データベースの中にテーブルが存在しているか確認する
            _df = TimescaleDBManager.read_sql_query(f"select * from information_schema.tables where table_name='{_table_name}'", cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'TimebarManager.init_database(database_name = {cls.__name__}, table_name = {_table_name}) : Database init failed. Exception {e}')
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
            TimescaleDBManager.execute_sql(_sql, cls.__name__)
        except Exception as e:
            AsyncManager.log_error(f'TimebarManager.init_database(database_name = {cls.__name__}, table_name = {_table_name}) : Create table failed. Exception {e}')
            raise(e)
    
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
        assert TimebarManager._instance is not None
        assert TimebarManager._instance._timebar_interval is not None

        _keys = list(cls._timebar_interval_dict.keys())
        _interval_sec: int = int(TimebarManager._instance._timebar_interval.total_seconds())

        assert _interval_sec in _keys, f"get_table_name(): {_interval_sec} is not in {_keys}"

        _interval_str = cls._timebar_interval_dict[_interval_sec]

        return f'{ExchangeManager.get_exchange_name()}_timebar_{_interval_str}'.lower()
    
    @classmethod
    def get_latest_timebar(cls, symbol: str = None, limit: int = 1):
        """
        DB上の最新のタイムバーをlimitで指定した本数取得する
        
        Parameters
        ----------
        symbol : str
            (必須) シンボル名
        limit: int
            読み込む行数

        Returns
        ----------
        pd.DataFrame
            該当するタイムバーが存在する
        None
            該当するタイムバーが存在しない
        """
        assert symbol is not None
        assert limit >= 1

        _table_name = cls.get_table_name()
        
        try:
            _df = TimescaleDBManager.read_sql_query(f'SELECT * FROM "{_table_name}" WHERE symbol = \'{symbol}\' ORDER BY datetime DESC LIMIT {limit}', cls.__name__, dtype = str)
        except Exception as e:
            AsyncManager.log_error(f'TimebarManager.get_latest_timebar(symbol = {symbol}) : Read failed. Exception {e}')

        if len(_df) > 0:
            _to_decimal = lambda x: Decimal(x)
            _df['datetime'] = pd.to_datetime(_df['datetime'], format='%Y-%m-%d %H:%M:%S%z')                
            _df['datetime_to'] = pd.to_datetime(_df['datetime_to'], format='%Y-%m-%d %H:%M:%S%z')
            _df['open'] = _df['open'].apply(_to_decimal)
            _df['high'] = _df['high'].apply(_to_decimal)
            _df['low'] = _df['low'].apply(_to_decimal)
            _df['close'] = _df['close'].apply(_to_decimal)
            _df['mark_open'] = _df['mark_open'].apply(_to_decimal)
            _df['mark_high'] = _df['mark_high'].apply(_to_decimal)
            _df['mark_low'] = _df['mark_low'].apply(_to_decimal)
            _df['mark_close'] = _df['mark_close'].apply(_to_decimal)
            _df['base_volume'] = _df['base_volume'].apply(_to_decimal)
            _df['quote_volume'] = _df['quote_volume'].apply(_to_decimal)
            _df['quote_buy_volume'] = _df['quote_buy_volume'].apply(_to_decimal)
            _df['quote_lqd_volume'] = _df['quote_lqd_volume'].apply(_to_decimal)
            _df['quote_lqd_buy_volume'] = _df['quote_lqd_buy_volume'].apply(_to_decimal)
            _df['quote_volume_cumsum'] = _df['quote_volume_cumsum'].apply(_to_decimal)
            _df['quote_buy_volume_cumsum'] = _df['quote_buy_volume_cumsum'].apply(_to_decimal)
            return _df.sort_values('datetime', ascending = True)
        
        return None

    @classmethod
    def get_interval_str(cls, interval: timedelta = None) -> str:
        """
        与えられたtimedeltaをBinance APIで利用できるタイムバー間隔を指定する文字列に変換する関数
        
        Parameters
        ----------
        interval : timedelta
            (必須) タイムバー間隔
        
        Returns
        -------
        str
            BinanceのWebSocket APIが利用できるタイムバー間隔指定文字列
        """
        assert TimebarManager._instance is not None
        assert interval is not None

        _keys = list(TimebarManager._timebar_interval_dict.keys())
        _interval_sec: int = int(interval.total_seconds())

        assert _interval_sec in _keys, f"get_interval_str(): {_interval_sec} is not in {_keys}"

        return TimebarManager._timebar_interval_dict[_interval_sec]

    @classmethod
    async def _get_timebar_from_api_async(cls, symbol: str = None, since: int = None, limit: int = 1, markprice = False):
        """
        指定されたシンボルのタイムバー情報を取得する関数
        パラメータ
        ----------
        symbol : str
            (必須) シンボル名
        since : int
            タイムバーを取得し始める時間 (ミリ秒)
        limit : int
            取得するタイムバーの最大数。デフォルト値 = 1
        markprice : boolean
            マーク価格を取得するか否か。デフォルト値 = False
        返り値
        -------
        Binanceから受信したタイムバー配列あるいはNone
        """
        assert cls._timebar_interval is not None

        _params = {}
        _params['symbol'] = symbol
        _params['interval'] = TimebarManager.get_interval_str(cls._timebar_interval)
        _params['limit'] = limit
        if since is not None:
            _params['startTime'] = since
        
        # 使用するAPIウェイトを計算
        if limit < 100:
            _api_weight = 1
        elif limit < 500:
            _api_weight = 2
        elif limit <= 1000:
            _api_weight = 5
        else:
            _api_weight = 10 
        
        # 取得成功までリトライを繰り返す
        _retry_count = 0
        _retry_max = 3

        # PyBottersクライアントのインスタンスを取得しておく
        _client: pybotters.Client = PyBottersManager.get_client()

        while True:
            try:
                _api_use_result = ExchangeManager.use_api_weight(_api_weight)
                if _api_use_result == True:
                    if markprice == True:
                        _url = '/fapi/v1/markPriceKlines'
                    else:
                        _url = '/fapi/v1/klines'

                    _r = await _client.get(_url, params = _params)
                    
                    # リクエスト結果による分岐
                    if _r.status == 200:
                        break
                    else:
                        # 200以外は1秒待ってリトライ
                        _text = await _r.text()
                        AsyncManager.log_warning(f'TimebarManager._get_timebar_from_api_async() : {symbol} Retry. Status ({_r.status}) : {_text}')
                        await asyncio.sleep(1.0)
                        _retry_count = _retry_count + 1
            except Exception as e:
                # 例外は1秒待ってリトライ
                AsyncManager.log_warning(f'TimebarManager._get_timebar_from_api_async() : {symbol} Retry. Exception : {e}')
                await asyncio.sleep(1.0)
                _retry_count = _retry_count + 1
            
            # リトライ回数のチェック
            if _retry_count > _retry_max:
                return None
        return _r

    @classmethod
    async def _update_kline_db_async(cls, symbol: str = None, interval: timedelta = None) -> bool:
        """
        指定されたシンボルのタイムバー情報を取得する関数
        
        Parameters
        ----------
        symbol : str
            (必須) シンボル名
        interval : int
            (必須) タイムバー間隔
        
        Returns
        -------
        True
            新しいタイムバーを読み込んでDBに記録した
        False
            新しいタイムバーはなかった
        """
        assert cls._timebar_interval is not None

        _table_name = cls.get_table_name()

        # 一度でも新しいバーを読み込んだかを記録するフラグ
        _updated: bool = False

        # どの時間からダウンロードを開始するか等のデフォルト値
        _since: int = 0
        _quote_volume_cumsum_offset: Decimal = Decimal(0)
        _quote_buy_volume_cumsum_offset: Decimal = Decimal(0)

        _df: pd.DataFrame = None

        try:
            _df_latest: pd.DataFrame = cls.get_latest_timebar(symbol, 1)
        except Exception as e:
            AsyncManager.log_error(f'TimebarManager._update_kline_db_async() : Exception when getting the latest {symbol} timebar : {e}')
            return False
        
        # _dfが読み込めている場合のみダウンロード開始時間等を最新のタイムバーの値に基づいて更新する
        if type(_df_latest) != type(None):
            _latest_timebar = _df_latest.iloc[-1]
            _latest_timestamp = int(_latest_timebar['datetime'].timestamp() * 1000)
            _since = _latest_timestamp + 1
            _quote_volume_cumsum_offset = _latest_timebar['quote_volume_cumsum']
            _quote_buy_volume_cumsum_offset = _latest_timebar['quote_buy_volume_cumsum']

        _origin = _since

        while True:
            _r = await cls._get_timebar_from_api_async(symbol, _since, 499)
            if _r is None:
                # 何らかの理由で全くデータを取得できなかったので処理を中断する
                break

            _list_timebar = await _r.json()
            if len(_list_timebar) <= 0:
                # 長さ0のデータを取得したので処理を中断する
                break

            _list_dict_for_timebar = []
            for _item in _list_timebar:
                _list_dict_for_timebar.append(cls._convert_timebar_list_to_timescaledb_dict(_item))
            _df_timebar = pd.DataFrame.from_dict(_list_dict_for_timebar, dtype = object)
            _df_timebar.set_index('datetime', drop = True, inplace = True)

            # マーク価格を取得する
            _r = await cls._get_timebar_from_api_async(symbol, _since, 499, True)
            if _r is None:
                # 何らかの理由で全くデータを取得できなかったので処理を中断する
                break

            _list_markprice = await _r.json()
            if len(_list_markprice) <= 0:
                # 長さ0のデータを取得したので処理を中断する
                break

            _list_dict_for_markprice = []
            for _item in _list_markprice:
                _list_dict_for_markprice.append(cls._convert_timebar_list_to_timescaledb_dict(_item, True))
            _df_markprice = pd.DataFrame.from_dict(_list_dict_for_markprice, dtype = object)
            _df_markprice.set_index('datetime', drop = True, inplace = True)

            _df = _df_timebar.join(_df_markprice, how = 'inner', sort = True)

            if _df.empty == True and _df_timebar.empty == False and _df_markprice.empty == False:
                # _df_timebarと_df_markpriceの時間がズレているので_dfがemptyになっている
                # sinceを調整して再チャレンジする
                _start_timebar = int(_df_timebar.index[0].timestamp() * 1000)
                _start_markprice = int(_df_markprice.index[0].timestamp() * 1000)
                _since = max(_start_timebar, _start_markprice) - 1
                continue    
            
            _df['quote_lqd_volume'] = Decimal(0)
            _df['quote_lqd_buy_volume'] = Decimal(0)
            _df['quote_volume_cumsum'] = _df['quote_volume'].cumsum() + _quote_volume_cumsum_offset
            _df['quote_buy_volume_cumsum'] = _df['quote_buy_volume'].cumsum() + _quote_buy_volume_cumsum_offset
            _df['symbol'] = symbol

            # まだクローズしていないタイムバーを除外する
            _df = _df[_df['datetime_to'] < datetime.now(tz = timezone.utc) - timedelta(seconds = 5)]

            # DBに入れるためにインデックスをカラムに戻す
            _df.reset_index(inplace = True)

            if len(_df) > 0:
                try:
                    # データベースに新しいタイムバーを書き込む
                    TimescaleDBManager.df_to_sql(_df, cls.__name__, schema = _table_name, if_exists = 'append')
                except Exception as e:
                    # データベース書き込みで失敗したのでただちに処理を中断する。取得できていたタイムバーはDBに書き込まれない
                    AsyncManager.log_error(f'TimebarManager._update_kline_db_async() : Exception when writing {symbol} timebar to DB: {e}')
                    raise(e)
                
                # 更新フラグやcumsum系のオフセットを更新してもう一度タイムバー取得を試みる
                _updated = True
                if _origin == 0:
                    # 仮のoriginを使っていた場合は、今回読み込んだタイムバーで一番古いものを以後_originとして利用してプログレスバーの表示に利用する
                    _origin = int(_df.iat[0, _df.columns.get_loc('datetime')].timestamp() * 1000 + 1)

                _since = int(_df.iat[-1, _df.columns.get_loc('datetime')].timestamp() * 1000 + 1)
                
                _quote_volume_cumsum_offset = _df.iat[-1, _df.columns.get_loc('quote_volume_cumsum')]
                _quote_buy_volume_cumsum_offset = _df.iat[-1, _df.columns.get_loc('quote_buy_volume_cumsum')]

                gc.collect()
            else:
                break        
        return _updated
    
    @classmethod
    async def _update_all_klines_loop_async(cls):
        """
        タイムバーの取得とDBへの保存、コールバック関数の呼び出しを一定期間おきに繰り返す無限ループ関数

        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        assert TimebarManager._instance is not None
        
        _instance: TimebarManager = TimebarManager._instance
        _last_download_second_idx = 0

        # _timebar_interval足を永久に取り続けるループ
        while True:
            _now_timestamp = datetime.now(timezone.utc).timestamp()
            _seconds = int(_now_timestamp)
            
            # 時間足更新時間から10秒の余裕を取る
            _timebar_idx = int((_seconds - 10) / _instance._timebar_interval.total_seconds())

            if _timebar_idx > _last_download_second_idx:
                # 取引所の銘柄情報を最新のものに更新し、銘柄情報を取得する
                await ExchangeManager.update_exchangeinfo_async()
                _all_symbols = ExchangeManager.get_all_symbols()

                AsyncManager.log_info(f'TimebarManager._update_all_klines_loop_async() : Downloading timebars : _second_idx = {_timebar_idx}, symbol_num = {len(_all_symbols)}')
                _last_download_second_idx = _timebar_idx
                
                # 5分足をダウンロードしてDBに保存する
                _update_tasks = [_instance._update_kline_db_async(_symbol, True) for _symbol in _all_symbols]
                _rs = await asyncio.gather(*_update_tasks, return_exceptions = True)

                for _r in _rs:
                    if isinstance(_r, Exception):
                        AsyncManager.log_warning(f'TimebarManager._update_all_klines_loop_async() : Exception raised {_r}')

                AsyncManager.log_info(f'TimebarManager._update_all_klines_loop_async() : Download completed')
                
                # Kafkaに全シンボルのダウンロードが終わったことを
                await TimebarManager._kafka_producer.send_and_wait(cls.__name__, f'ALL : download completed'.encode('utf-8'))

            await asyncio.sleep(1.0)

if __name__ == "__main__":
    # 簡易的なテストコード
    from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis
    
    async def test():
        # AsyncManagerの初期化
        await AsyncManager.init_async()

        # TimebarManagerの初期化前に、TimescaleDBManagerの初期化が必要
        await TimescaleDBManager.init_async(pg_config)
        
        # TimebarManagerの初期化前に、PyBottersManagerの初期化が必要
        _exchange_config = binance_config.copy()
        _pybotters_params = _exchange_config.copy()
        _pybotters_params['apis'] = pybotters_apis.copy()
        await PyBottersManager.init_async(_pybotters_params)

        # タイムバーをダウンロードするだけなら、run_asyncを読んでWebsocket APIからポジション情報等をダウンロードする必要はない
        await ExchangeManager.init_async(_exchange_config)

        # TimebarManagerの初期化
        _timebar_params = {
            'timebar_interval': timedelta(minutes = 5)
        }
        await TimebarManager.init_async(_timebar_params)

        # タイムバーをダウンロードする非同期タスクを起動する
        await TimebarManager.run_async()

        # 無限ループ
        while True:
            await asyncio.sleep(60.0)
    
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        pass
