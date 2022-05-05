from abc import abstractmethod, ABC
from logging import Logger

class AsyncManager(ABC):
    # インスタンスを保持するクラス変数
    _instance: object = None

    # 非同期タスクを中断するためのフラグ
    _abort_async: bool = False

    # 全ての派生クラスで共有されるロガー
    _logger: Logger = None

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
    async def init_async(cls, params: dict = None, logger: Logger = None) -> None:
        """
        AsyncManagerの初期化用抽象メソッド
        
        Parameters
        ----------
        なし

        Returns
        -------
        なし。失敗した場合は例外をRaiseする。
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
