import os

from mode import backtest
from mode import live


# docker-compose 에서 argument 전달
try:
    strategy_name = os.environ["strategy"]
except Exception:
    strategy_name = "junggil_003"
try:
    mode_str = os.environ["mode"]
except Exception:
    mode_str = "backtest"


if mode_str == "backtest":
    # 백테스트
    argos = backtest.BacktestMode(strategy_name, False)
elif mode_str == "live_paper":
    # 라이브 페이퍼
    argos = live.LiveMode(strategy_name, is_live=False)
elif mode_str == "live":
    # 라이브
    argos = live.LiveMode(strategy_name, is_live=True)
    from common import helper

    helper.enable_slack = True
else:
    raise ("invalid mode")
argos.run()
