from . import base


class DrawDown(base.Base):
    def __init__(self, symbol, order_handler):
        base.Base.__init__(self, order_handler)

        self.symbol = symbol
        self.max_value = 0.0
        self.drawdown = 0.0
        self.max_drawdown = 0.0
        self.drawn_period = 0.0
        self.max_drawdown_period = 0.0

    def on_data(self, df):
        value = 0.0
        if not self.symbol:
            value = self.order_handler.get_value()
        else:
            value = self.order_handler.get_value(self.symbol)

        self.max_value = max(self.max_value, value)

        moneydown = self.max_value - value
        self.drawdown = 100 * moneydown / self.max_value
        self.max_drawdown = max(self.max_drawdown, self.drawdown)

        if self.drawdown > 0.0:
            self.drawn_period += 1.0
        else:
            self.drawn_period = 0.0

        self.max_drawdown_period = max(self.max_drawdown_period, self.drawn_period)
