from abc import ABC, abstractmethod
from common import arg
from common import log


class Base(ABC):
    def __init__(self, strategy_name: str, is_live: bool):
        self.args = arg.create_args(strategy_name, is_live)

        # 기본 로깅 형태 지정. 데이터 도착 시점 부터는 현재시간 -> 데이터 날짜로 변경한다.
        self.logging = log.makeLogger(strategy_name)

    @abstractmethod
    def run(self, variables=list()):
        pass
