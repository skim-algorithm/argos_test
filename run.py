from mode import backtest
from mode import live
import ccxt
import sys

# For Testing
def fetch_binance_futures_positions():
    """
    Fetch and print the current positions from Binance Futures using ccxt.
    """
    exchange = ccxt.binance({
        'apiKey': '',  # Replace with your Binance API key
        'secret': '',  # Replace with your Binance Secret key
        'options': {
            'defaultType': 'future',  # Specify to use futures endpoints
        },
    })
    try:
        positions = exchange.fetch_positions()
        print("Current Positions:")
        for position in positions:
            if float(position['contracts']) != 0:  # Filter out zero contracts
                print(position)
    except Exception as e:
        print(f"An error occurred while fetching positions: {e}")

if __name__ == "__main__":
    # 개발 편의를 위해 인자 값이 전달되지 않은 경우엔 아래 하드코딩된 값을 사용한다.
    strategy_name = "skim_001"
    mode_str = "live"
    #fetch_binance_futures_positions()

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
    elif mode_str == "live":
        # 라이브
        argos = live.LiveMode(strategy_name, is_live=True)
        from common import helper

        helper.enable_slack = True

        # Fetch Binance Futures current positions
    else:
        raise Exception("invalid mode")

    argos.run()


