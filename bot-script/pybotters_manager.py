from multiprocessing.connection import Client
import pybotters

from async_manager import AsyncManager

class PyBottersManager(AsyncManager):
    # グローバル共有のインスタンスを保持するクラス変数
    _instance: object = None

    # PyBottersのインスタンスを保持するクラス変数
    _client: pybotters.Client = None

    def __init__(self, params):
        """
        TimebarManagerコンストラクタ
        
        Parameters
        ----------
        なし
        
        Returns
        -------
        なし
        """
        assert params['rest_baseurl'] is not None
        assert params['apis'] is not None

        PyBottersManager._client = pybotters.Client(base_url = params['rest_baseurl'], apis = params['apis'])
        PyBottersManager._instance = self

    @classmethod
    def get_client(cls) -> pybotters.Client:
        return PyBottersManager._client

    @classmethod
    async def run_async(cls) -> None:
        """
        このマネージャーの非同期タスクループ起動用メソッド。利用しない。
        """
        pass

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
        # このマネージャーは自分用のテーブルを持たない
        return None
    
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
        # このマネージャーは自分用のデータベースを持たない
        return


if __name__ == "__main__":
    # 簡易的なテストコード
    import asyncio
    import logging
    from logging import Logger, getLogger, basicConfig
    from rich.logging import RichHandler

    _richhandler = RichHandler(rich_tracebacks = True)
    _richhandler.setFormatter(logging.Formatter('%(message)s'))
    basicConfig(level = logging.DEBUG, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
    _logger: Logger = getLogger('rich')
    AsyncManager.set_logger(_logger)
    
    from crypto_bot_config import exchange_config, pybotters_apis
    
    async def test():
        config = exchange_config.copy()
        config['apis'] = pybotters_apis.copy()
        await PyBottersManager.init_async(config)
        _client = PyBottersManager.get_client()
        _r = await _client.get('/fapi/v1/exchangeInfo')
        AsyncManager.log_info(await _r.json())
        await asyncio.sleep(60.0)
    
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        pass



