from abc import abstractmethod, ABC

import logging
from logging import Logger, getLogger, basicConfig
from rich.logging import RichHandler

class AsyncManager(ABC):
    # 全ての派生クラスで共有されるロガー
    _logger: Logger = None

    # 全ての派生クラスで利用されるログ系のクラスメソッド
    @classmethod
    def log_debug(cls, msg: str = None):
        assert msg is not None
        
        if AsyncManager._logger is not None:
            AsyncManager._logger.debug(msg)

    @classmethod
    def log_info(cls, msg: str = None):
        assert msg is not None
        
        if AsyncManager._logger is not None:
            AsyncManager._logger.info(msg)

    @classmethod
    def log_warning(cls, msg: str = None):
        assert msg is not None
        
        if AsyncManager._logger is not None:
            AsyncManager._logger.warning(msg)

    @classmethod
    def log_error(cls, msg: str = None):
        assert msg is not None
        
        if AsyncManager._logger is not None:
            AsyncManager._logger.error(msg)

    @classmethod
    def log_critical(cls, msg: str = None):
        assert msg is not None
        
        if AsyncManager._logger is not None:
            AsyncManager._logger.critical(msg)

    @classmethod
    def set_logger(cls, logger: Logger = None) -> None:
        """
        AsyncManagerのロガー設定メソッド
        
        Parameters
        ----------
        logger : Logger
            (必須) ロガー

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        assert logger is not None
        cls._logger = logger
    
    @classmethod
    @abstractmethod
    async def init_async(cls) -> None:
        """
        AsyncManagerの初期化用抽象メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        # Loggerの設定
        _richhandler = RichHandler(rich_tracebacks = True)
        _richhandler.setFormatter(logging.Formatter('%(message)s'))
        basicConfig(level = logging.DEBUG, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
        AsyncManager._logger: Logger = getLogger('rich')

    @classmethod
    @abstractmethod
    def init_database(cls, force: bool):
        """
        このマネージャーが利用するDBとテーブルの初期化用抽象メソッド
        
        Parameters
        ----------
        force : bool
            強制的にテーブルを初期化するか否か

        Returns
        ----------
        なし。失敗した場合は例外をRaiseする
        """
        pass      

    @classmethod
    @abstractmethod
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
        pass

    @classmethod
    @abstractmethod
    async def run_async(cls, params: dict, logger: Logger = None) -> None:
        """
        AsyncManagerの非同期タスクループ起動用抽象メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
        """
        pass
