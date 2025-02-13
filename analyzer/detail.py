import pandas as pd

from . import base


class Detail(base.Base):
    def __init__(self, analyzer, symbol, order_handler):
        base.Base.__init__(self, order_handler)

        self.analyzer = analyzer
        self.symbol = symbol

        self.data = pd.DataFrame()

        # 기본 정보
        self.position_sizes = []
        self.position_values = []
        self.cash_values = []
        self.prev_portfolio_value = None
        self.portfolio_values = []
        self.all_portfolio_values = []
        self.all_cash_values = []

        # PnL
        self.pnl_values = []
        self.cum_pnl_values = []

        # Returns
        self.return_values = []
        self.cum_return_values = []

        # TurnOver
        self.turnover_values = []

        # Sharpe
        # self.sharpe_values = []

        # Trade
        self.win_rate_values = []
        self.number_of_wins = []
        self.number_of_closed_orders = []
        self.number_of_loses = []
        self.total_loses = []
        self.total_profit = []

        # P/L Ratio
        self.pnl_ratio_values = []

        # Drawdown
        self.drawdown_values = []
        self.drawn_period_values = []
        self.max_drawdown_values = []

    def on_data(self, df):
        import pandas as pd

        ohlcv = df.iloc[-1]
        #self.data = self.data.append(ohlcv)
        self.data = pd.concat([self.data, ohlcv], ignore_index=True)
        close_price = ohlcv["close"]

        # 기본 정보
        if pos := self.order_handler.get_position(self.symbol):
            self.position_sizes.append(pos.quantity)
            self.position_values.append(pos.quantity * close_price)
            self.cash_values.append(self.order_handler.usd - abs(pos.cost))
        else:
            self.position_sizes.append(0.0)
            self.position_values.append(0.0)
            self.cash_values.append(self.order_handler.usd)

        self.all_cash_values.append(self.order_handler.usd - abs(self.order_handler.all_pos_cost))

        portfolio_value = self.order_handler.get_value(self.symbol)
        self.portfolio_values.append(portfolio_value)
        self.all_portfolio_values.append(self.order_handler.get_value())

        # PnL
        # pnl = self.analyzer.analyzers[self.symbol]["total"]["periodstats"].pnl_value
        pnl = self.position_values[-1] - self.position_values[-2] if len(self.position_values) > 1 else 0.0
        self.pnl_values.append(pnl)
        prev_pnl = self.cum_pnl_values[-1] if len(self.cum_pnl_values) > 0 else 0
        self.cum_pnl_values.append(prev_pnl + pnl)

        # Returns
        return_value = self.analyzer.analyzers[self.symbol]["total"]["periodstats"].get_last_return_value() * 100
        cum_return_value = (
            (1 + self.cum_return_values[-1]) * (1 + return_value) - 1 if len(self.cum_return_values) > 0 else 0.0
        )
        self.return_values.append(return_value)
        self.cum_return_values.append(cum_return_value)

        # TurnOver
        turnover_value = self.analyzer.analyzers[self.symbol]["total"]["turnover"].daily_turnover * 100
        self.turnover_values.append(turnover_value)

        # Sharpe
        # 데이터프레임별로 sharpe_ratio를 계산하는 것은 비용이 매우 크다.
        # 추후 sharepe_ratio가 필요하다면 점진적으로 계산하는 다른 방법을 찾아야 한다.
        # sharpe_ratio = self.analyzer.analyzers[self.symbol]["total"]["periodstats"].calculate_sharpe_ratio()
        # self.sharpe_values.append(sharpe_ratio)

        # Trade
        win_rate = self.analyzer.analyzers[self.symbol]["total"]["trade"].calculate_win_rate()
        self.win_rate_values.append(win_rate)

        self.number_of_wins.append(self.analyzer.analyzers[self.symbol]["total"]["trade"].number_of_wins)
        self.number_of_loses.append(self.analyzer.analyzers[self.symbol]["total"]["trade"].number_of_loses)
        self.total_profit.append(self.analyzer.analyzers[self.symbol]["total"]["trade"].total_profit)
        self.total_loses.append(self.analyzer.analyzers[self.symbol]["total"]["trade"].total_loses)
        self.number_of_closed_orders.append(
            self.analyzer.analyzers[self.symbol]["total"]["trade"].number_of_closed_orders
        )

        pnl_ratio = self.analyzer.analyzers[self.symbol]["total"]["trade"].calculate_pnl_ratio()
        self.pnl_ratio_values.append(pnl_ratio)

        # DrawDown
        drawdown = self.analyzer.analyzers[self.symbol]["total"]["drawdown"].drawdown
        drawn_period = self.analyzer.analyzers[self.symbol]["total"]["drawdown"].drawn_period
        max_drawdown = self.analyzer.analyzers[self.symbol]["total"]["drawdown"].max_drawdown
        self.drawdown_values.append(drawdown)
        self.drawn_period_values.append(drawn_period)
        self.max_drawdown_values.append(max_drawdown)

        self.prev_portfolio_value = portfolio_value
