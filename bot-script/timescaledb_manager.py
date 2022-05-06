from logging import Logger

from psycopg2 import Timestamp
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database, drop_database

import pandas as pd
from pandas import DataFrame

from async_manager import AsyncManager

class TimescaleDBManager(AsyncManager):
    # インスタンスを保持するクラス変数
    _instance: object = None

    # 非同期タスクを中断するためのフラグ
    _abort_async: bool = None

    def __init__(self, params: dict = None, logger: Logger = None):
        """
        TimescaleDBManagerコンストラクタ。外部から直接呼ばれることはない
        
        Parameters
        ----------
        params : dict
            (必須) 初期化パラメータが入った辞書
        params['user'] : str
            (必須) TimeScaleDBのユーザー名
        params['password'] : str
            (必須) TimeScaleDBのパスワード
        params['host'] : str
            (必須) TimeScaleDBのホスト名
        params['port'] : str
            (必須) TimeScaleDBのポート番号
        logger : Logger
            ロガー

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert params is not None
        assert params['user'] is not None
        assert params['password'] is not None
        assert params['host'] is not None
        assert params['port'] is not None
        assert TimescaleDBManager._instance is None

        # ユーザー等の値を保存しておく
        self._user = params['user']
        self._password = params['password']
        self._host = params['host']
        self._port = params['port']
        
        # DB接続をデータベース名ごとに格納する辞書
        self._engines: dict = {}

        _sqlalchemy_config = f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}:{params['port']}/postgres"
        self._engines['postgres'] = create_engine(_sqlalchemy_config, pool_pre_ping = True)

        #_sqlalchemy_config = f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

        if logger:
            self._logger = logger
    
    @classmethod
    async def init_async(cls, params: dict = None, logger: Logger = None) -> None:
        """
        TimescaleDBManagerの初期化関数
        
        Parameters
        ----------
        params : dict
            (必須) 初期化パラメータが入った辞書。内容は__init__を参照
        logger : Logger
            ロガー

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert TimescaleDBManager._instance is None

        TimescaleDBManager._instance = TimescaleDBManager(params, logger)

        return

    @classmethod
    async def run_async(cls) -> None:
        """
        TimescaleDBManagerの非同期タスクループ起動用メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        # 今のところ非同期バックグラウンドタスクはないので空の関数としておく
        return

    @classmethod
    def init_database(cls, db_name: str = None, force: bool = False) -> None:
        """
        データベース初期化メソッド
        
        Parameters
        ----------
        db_name : str
            (必須) DB名
        force : bool
            強制的にDBを初期化するか否か

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert TimescaleDBManager._instance._engines['postgres'] is not None

        # DB接続エンジンを初期化
        _instance = TimescaleDBManager._instance
        _sqlalchemy_config = f"postgresql+psycopg2://{_instance._user}:{_instance._password}@{_instance._host}:{_instance._port}/{db_name}"
        _instance._engines[db_name] = create_engine(_sqlalchemy_config, pool_pre_ping = True)

        # データベースの存在を確認
        if database_exists(_instance._engines[db_name].url):
            if force:
                drop_database(_instance._engines[db_name].url)
            else:
                return
        
        # データベースを新規作成 (自動的にTimescaleDB拡張が有効になっている)
        create_database(_instance._engines[db_name].url)

        # enum_side型がデータベース上に存在することを確認し、ない場合は作成する
        _r = TimescaleDBManager.execute_sql("SELECT * from pg_type WHERE typname='enum_side'", db_name)
        if _r.rowcount == 0:
            cls.execute_sql("CREATE TYPE enum_side AS ENUM ('buy', 'sell')", db_name)


    @classmethod
    def execute_sql(cls, sql: str = None, db_name: str = None) -> dict:
        """
        指定されたSQLを実行し、結果を辞書で返すメソッド
        
        Parameters
        ----------
        sql : str
            (必須) 実行するSQL文
        db_name : str
            (必須) データベース名
        
        Returns
        -------
        dict
            SQLクエリ結果の辞書。
        """
        assert sql is not None
        assert db_name is not None
        assert TimescaleDBManager._instance is not None

        return cls._instance._engines[db_name].execute(sql)
        
    @classmethod
    def read_sql_query(cls, sql: str = None, db_name: str = None, index_column: str = '', dtype: dict = {}) -> DataFrame:
        """
        指定されたSQLを実行し、結果をデータフレームで返すメソッド
        
        Parameters
        ----------
        sql : str
            (必須) 実行するSQL文。
        db_name : str
            (必須) データベース名
        index_column : str
            出力するデータフレームのインデックスにする列名 (デフォルト値'')
        dtype : dict
            読みだしたデータの型を指定する (デフォルト値 {})
        
        Returns
        -------
        df : pandas.DataFrame
            SQLクエリ結果のデータフレーム。
        """
        assert sql is not None
        assert db_name is not None
        assert TimescaleDBManager._instance is not None
        
        df = pd.read_sql_query(sql, cls._instance._engines[db_name], dtype = dtype)
        if len(index_column) > 0:
            df = df.set_index(index_column, drop = True)
        return df
    
    @classmethod
    def df_to_sql(cls, df = None, db_name: str = None, schema: str = None, if_exists: str = 'fail'):
        """
        指定されたデータフレームをSQLデータベースに追加して結果を返すメソッド
        
        Parameters
        ----------
        df : pandas.DataFrame
            (必須) データベースに追加するデータを含んだデータフレーム
        db_name : str
            (必須) データベース名
        schema : str
            (必須) データベースのスキーマ名
        if_exists : すでに書き込もうとするスキーマあった場合の処理
            'fail' : 失敗させる
            'append' : テーブルに追記する
            'replace' : テーブルを削除し、書き込もうとしているデータで作り直す
        
        Returns
        -------
        int
            変更された行数
        """
        assert df is not None and df.empty == False
        assert db_name is not None
        assert schema is not None
        assert cls._instance is not None
        
        return df.to_sql(schema, con = cls._instance._engines[db_name], if_exists = if_exists, index = False)

if __name__ == "__main__":
    # 簡易的なテストコード
    from os import environ
    import asyncio
    from crypto_bot_config import pg_config
    import logging
    from logging import Logger, getLogger, basicConfig
    from rich.logging import RichHandler

    _richhandler = RichHandler(rich_tracebacks = True)
    _richhandler.setFormatter(logging.Formatter('%(message)s'))
    basicConfig(level = logging.DEBUG, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
    _logger: Logger = getLogger('rich')
    AsyncManager.set_logger(_logger)
    
    async def test():
        await TimescaleDBManager.init_async(pg_config)
        TimescaleDBManager.init_database('timebar', True)
    
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        pass
