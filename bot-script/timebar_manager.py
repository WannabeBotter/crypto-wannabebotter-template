import gc
import asyncio
from typing import Callable
import aiohttp
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

from logging import Logger

import pybotters

from timescaledb_manager import TimescaleDBManager
from exchange_manager import ExchangeManager

class TimebarManager:
    _instance = None
    
    @classmethod
    def _convert_timebar_list_to_websocket_dict(self, symbol: str=None, timebar_list: list=None) -> dict:
        """
        Binance REST APIで得られたリスト形式のタイムバーをBinance Websocket APIで得られる辞書型形式に変換する関数

        Parameters
        ----------
        symbol : str
            (必須) シンボル名
        timebar_list : list
            (必須) 変換元のリスト形式のタイムバー
        
        Returns
        -------
        dict
            BinanceのWebSocket APIが返す辞書型形式で表現された1本のタイムバー
        """
        assert symbol is not None, "Symbol must not None"
        assert timebar_list is not None, "timebar_list must not None"
        assert len(timebar_list) == 12, "Timebar list length isn't 12"

        _duration = ((timebar_list[6] + 1) - timebar_list[0]) // 1000
        _duration_str = TimescaleDBManager.convert_interval_to_str(timedelta(seconds = _duration))
        
        return {
                't': timebar_list[0],
                'T': timebar_list[6],
                's': symbol,
                'i': _duration_str,
                'f': 0,
                'L': 0,
                'o': timebar_list[1],
                'h': timebar_list[2],
                'l': timebar_list[3],
                'c': timebar_list[4],
                'v': timebar_list[5],
                'n': timebar_list[8],
                'x': True,
                'q': timebar_list[7],
                'V': timebar_list[9],
                'Q': timebar_list[10],
                'B': timebar_list[11],
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
            'id': 0,
            'id_to': 0,
            'open': Decimal(timebar_list[1]),
            'high': Decimal(timebar_list[2]),
            'low': Decimal(timebar_list[3]),
            'close': Decimal(timebar_list[4]),
            'volume': Decimal(timebar_list[5]),
            'dollar_volume': Decimal(timebar_list[7]),
            'dollar_buy_volume': Decimal(timebar_list[10]),
            'dollar_sell_volume': Decimal(timebar_list[7]) - Decimal(timebar_list[10]),
            'dollar_liquidation_volume': Decimal(0),
            'dollar_liquidation_buy_volume': Decimal(0),
            'dollar_liquidation_sell_volume': Decimal(0),
            'dollar_cumsum': Decimal(0),
            'dollar_buy_cumsum': Decimal(0),
            'dollar_sell_cumsum': Decimal(0),
        }

    @classmethod
    async def init_async(cls, client: pybotters.Client = None, timescaledb_manager: TimescaleDBManager = None, exchange_manager: ExchangeManager = None,
                        timebar_interval: timedelta = None, cache_duration: timedelta = None, logger: Logger = None, async_job: bool = False) -> object:
        """
        TimebarManagerのインスタンスを作成しする非同期初期化関数
        
        Parameters
        ----------
        client : pybotters.Client
            (必須) PyBotters.Clientのインスタンス
        timescaledb_manager : TimescaleDBManager
            (必須) TimescaleDBManagerのインスタンス
        timebar_interval : timedelta
            (必須) このTimebarManagerで利用するタイムバーの間隔
        cache_duration : timedelta
            (必須) メモリ上のキャッシュにタイムバーを保持する持続時間
        logger : Logger
            (必須) ロガー

        Returns
        -------
        TimebarManager
            TimebarManagerのインスタンス
        """
        assert cls._instance is None

        logger.debug(f'TimebarManager._init_async(async_job = {async_job})')

        cls._instance = TimebarManager(client, timescaledb_manager, exchange_manager, timebar_interval, cache_duration, logger)
        assert cls._instance is not None

        if async_job:
            # 非同期ジョブを実行するフラグが指定されていたら、タイムバーをダウンロードするタスクを作成する
            asyncio.create_task(cls._instance._update_all_klines_loop_async())

        return cls._instance

    def __init__(self, client: pybotters.Client = None, timescaledb_manager: TimescaleDBManager = None, exchange_manager: ExchangeManager = None,
                 timebar_interval: timedelta = None, cache_duration: timedelta = None, logger: Logger = None):
        """
        TimebarManagerコンストラクタ
        
        Parameters
        ----------
        client : pybotters.Client
            (必須) PyBotters.Clientのインスタンス
        dbutil : timescaledb_util.TimeScaleDBUtil
            (必須) TimeScaleDBUtilのインスタンス
        exchange_manager : timescaledb_util.TimeScaleDBUtil
            (必須) ExchangeManagerのインスタンス
        timebar_interval : timedelta
            (必須) このTimebarManagerで利用するタイムバーの間隔 (秒)
        cache_duration : timedelta
            (必須) メモリ上のキャッシュにタイムバーを保持する持続時間
        logger : Logger
            (必須) ロガー

        Returns
        -------
        なし
        """
        assert client is not None
        assert timescaledb_manager is not None
        assert exchange_manager is not None
        assert timebar_interval is not None
        assert cache_duration is not None
        assert logger is not None

        self._logger = logger
        self._client = client
        self._timescaledb_manager = timescaledb_manager
        self._exchange_manager = exchange_manager

        self._timebar_interval:timedelta = timebar_interval
        self._cache_duration: timedelta = cache_duration

        self._asyncio_lock = asyncio.Lock()
        
    async def get_all_timebars_from_db(self, exchange_str: str = None, column: str = None, get_from: datetime = None, get_to: datetime = None):
        assert exchange_str is not None
        assert get_from is not None
        assert column is not None
        assert get_to is not None

        return None

    async def _get_timebar_from_api_async(self, symbol_str: str = None, since: int = None, limit: int = 1, markprice = False):
        """
        指定されたシンボルのタイムバー情報を取得する関数
        パラメータ
        ----------
        symbol_str : str
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
        assert self._timebar_interval is not None

        _params = {}
        _params['symbol'] = symbol_str
        _params['interval'] = TimescaleDBManager.convert_interval_to_str(self._timebar_interval)
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

        while True:
            try:
                _api_use_result = await self._exchange_manager.use_api_weight_async(_api_weight)
                if _api_use_result == True:
                    if markprice == True:
                        _r = await self._client.get(f'/fapi/v1/markPriceKlines', params=_params)
                    else:
                        _r = await self._client.get(f'/fapi/v1/klines', params=_params)
                    
                    # リクエスト結果による分岐
                    if _r.status == 200:
                        break
                    else:
                        # 200以外は1秒待ってリトライ
                        _text = await _r.text()
                        self._logger.warning(f'TimebarManager._get_timebar_from_api_async() : {symbol_str} Retry. Status ({_r.status}) : {_text}')
                        await asyncio.sleep(1.0)
                        _retry_count = _retry_count + 1
            except Exception as e:
                # 例外は1秒待ってリトライ
                self._logger.warning(f'TimebarManager._get_timebar_from_api_async() : {symbol_str} Retry. Exception : {e}')
                await asyncio.sleep(1.0)
                _retry_count = _retry_count + 1
            
            # リトライ回数のチェック
            if _retry_count > _retry_max:
                return None
        return _r

    async def _update_kline_db_async(self, symbol_str: str = None, interval: timedelta = None) -> bool:
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
        assert self._timebar_interval is not None

        # テーブルが存在することを確認、Falseならばテーブルは存在せず処理を継続できない
        if self._timescaledb_manager.init_timebar_table(self._exchange_manager.exchange_name, self._timebar_interval) == False:
            return False
        
        _table_name = self._timescaledb_manager.get_timebar_table_name(self._exchange_manager.exchange_name, self._timebar_interval)

        # 一度でも新しいバーを読み込んだかを記録するフラグ
        _updated: bool = False

        # どの時間からダウンロードを開始するか等のデフォルト値
        _since: int = 0
        _dollar_cumsum_offset: Decimal = Decimal(0)
        _dollar_buy_cumsum_offset: Decimal = Decimal(0)
        _dollar_sell_cumsum_offset: Decimal = Decimal(0)

        _df: pd.DataFrame = None

        try:
            _df_latest: pd.DataFrame = self._timescaledb_manager.get_latest_timebar(self._exchange_manager.exchange_name, symbol_str, self._timebar_interval, 1)
        except Exception as e:
            self._logger.error(f'TimebarManager._update_kline_db_async() : Exception when getting the latest {symbol_str} timebar : {e}')
            return False
        
        # _dfが読み込めている場合のみダウンロード開始時間等を最新のタイムバーの値に基づいて更新する
        if type(_df_latest) != type(None):
            _latest_timebar = _df_latest.iloc[-1]
            _latest_timestamp = int(_latest_timebar['datetime'].timestamp() * 1000)
            _since = _latest_timestamp + 1
            _dollar_cumsum_offset = _latest_timebar['dollar_cumsum']
            _dollar_buy_cumsum_offset = _latest_timebar['dollar_buy_cumsum']
            _dollar_sell_cumsum_offset = _latest_timebar['dollar_sell_cumsum']

        _origin = _since

        while True:
            _r = await self._get_timebar_from_api_async(symbol_str, _since, 499)
            if _r is None:
                # 何らかの理由で全くデータを取得できなかったので処理を中断する
                break

            _list_timebar = await _r.json()
            if len(_list_timebar) <= 0:
                # 長さ0のデータを取得したので処理を中断する
                break

            _list_dict_for_timebar = []
            for _item in _list_timebar:
                _list_dict_for_timebar.append(self._convert_timebar_list_to_timescaledb_dict(_item))
            _df_timebar = pd.DataFrame.from_dict(_list_dict_for_timebar, dtype = object)
            _df_timebar.set_index('datetime', drop = True, inplace = True)

            # マーク価格を取得する
            _r = await self._get_timebar_from_api_async(symbol_str, _since, 499, True)
            if _r is None:
                # 何らかの理由で全くデータを取得できなかったので処理を中断する
                break

            _list_markprice = await _r.json()
            if len(_list_markprice) <= 0:
                # 長さ0のデータを取得したので処理を中断する
                break

            _list_dict_for_markprice = []
            for _item in _list_markprice:
                _list_dict_for_markprice.append(self._convert_timebar_list_to_timescaledb_dict(_item, True))
            _df_markprice = pd.DataFrame.from_dict(_list_dict_for_markprice, dtype = object)
            _df_markprice.set_index('datetime', drop = True, inplace = True)

            _df = _df_timebar.join(_df_markprice, how = 'inner', sort = True)
            
            _df['dollar_cumsum'] = _df['dollar_volume'].cumsum() + _dollar_cumsum_offset
            _df['dollar_buy_cumsum'] = _df['dollar_buy_volume'].cumsum() + _dollar_buy_cumsum_offset
            _df['dollar_sell_cumsum'] = _df['dollar_sell_volume'].cumsum() + _dollar_sell_cumsum_offset
            _df['symbol'] = symbol_str

            # まだクローズしていないタイムバーを除外する
            _df = _df[_df['datetime_to'] < datetime.now(tz = timezone.utc) - timedelta(seconds = 5)]

            # DBに入れるためにインデックスをカラムに戻す
            _df.reset_index(inplace = True)

            if len(_df) > 0:
                try:
                    # データベースに新しいタイムバーを書き込む
                    self._timescaledb_manager.df_to_sql(df=_df, schema = _table_name, if_exists = 'append')
                except Exception as e:
                    # データベース書き込みで失敗したのでただちに処理を中断する。取得できていたタイムバーはDBに書き込まれない
                    break
                
                # 更新フラグやcumsum系のオフセットを更新してもう一度タイムバー取得を試みる
                _updated = True
                if _origin == 0:
                    # 仮のoriginを使っていた場合は、今回読み込んだタイムバーで一番古いものを以後_originとして利用してプログレスバーの表示に利用する
                    _origin = int(_df.iat[0, _df.columns.get_loc('datetime')].timestamp() * 1000 + 1)

                _since = int(_df.iat[-1, _df.columns.get_loc('datetime')].timestamp() * 1000 + 1)
                
                _dollar_cumsum_offset = _df.iat[-1, _df.columns.get_loc('dollar_cumsum')]
                _dollar_buy_cumsum_offset = _df.iat[-1, _df.columns.get_loc('dollar_buy_cumsum')]
                _dollar_sell_cumsum_offset = _df.iat[-1, _df.columns.get_loc('dollar_sell_cumsum')]

                gc.collect()
            else:
                break            

        return _updated
            
    async def _update_all_klines_loop_async(self):
        """
        タイムバーの取得とDBへの保存、コールバック関数の呼び出しを一定期間おきに繰り返す無限ループ関数

        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        assert self._timebar_interval is not None

        _last_download_second_idx = 0

        # self._timebar_interval足を永久に取り続けるループ
        while True:
            _now_timestamp = datetime.now(timezone.utc).timestamp()
            _seconds = int(_now_timestamp)
            
            # 時間足更新時間から10秒の余裕を取る
            _timebar_idx = int((_seconds - 10) / self._timebar_interval.total_seconds())

            if _timebar_idx > _last_download_second_idx:
                self._logger.info(f'TimebarManager._update_all_klines_loop_async() : Downloading timebars : _second_idx = {_timebar_idx}, symbol_num = {len(self._exchange_manager.set_all_components)}')
                _last_download_second_idx = _timebar_idx
                
                # 取引所の銘柄情報を最新のものに更新
                await self._exchange_manager.update_exchangeinfo_async()

                # 5分足をダウンロードしてDBに保存する
                _update_tasks = [self._update_kline_db_async(_symbol, True) for _symbol in self._exchange_manager.set_all_components]
                _rs = await asyncio.gather(*_update_tasks, return_exceptions = True)

                for _r in _rs:
                    if isinstance(_r, Exception):
                        self._logger.warning(f'TimebarManager._update_all_klines_loop_async() : Exception raised {_r}')

                self._logger.info(f'TimebarManager._update_all_klines_loop_async() : Download completed')
            
            await asyncio.sleep(1.0)