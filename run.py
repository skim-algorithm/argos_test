from mode import backtest
#from mode import live
import sys


if __name__ == "__main__":
    # 개발 편의를 위해 인자 값이 전달되지 않은 경우엔 아래 하드코딩된 값을 사용한다.
    strategy_name = "skim_005"
    mode_str = "backtest"

    if len(sys.argv) == 3:
        strategy_name = sys.argv[1]
        mode_str = sys.argv[2]

    if mode_str == "backtest":
        # 백테스트
        argos = backtest.BacktestMode(strategy_name, False)
    # elif mode_str == "backtest_simple":
    #     argos = backtest.BacktestMode(strategy_name, True)
    # elif mode_str == "live_paper":
    #     # 라이브 페이퍼
    #     argos = live.LiveMode(strategy_name, is_live=False)
    # elif mode_str == "live":
    #     # 라이브
    #     argos = live.LiveMode(strategy_name, is_live=True)
    #     from common import helper

    #     helper.enable_slack = True
    else:
        raise Exception("invalid mode")

    argos.run()
