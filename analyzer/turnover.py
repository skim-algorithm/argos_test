from . import base


class TurnOver(base.Base):
    def __init__(self, order_handler):
        base.Base.__init__(self, order_handler)

        self.daily_turnover = 0.0
        self.accum_turnover = 0.0
        self.daily_turnover_count = 0

        self.daily_accum_cost = 0.0

        self.turnover = 0.0

    def on_data(self, df):
        self.daily_turnover = self.daily_accum_cost / self.order_handler.get_value()
        self.accum_turnover += self.daily_turnover
        self.daily_accum_cost = 0.0
        self.daily_turnover_count += 1

    def on_order_done(self, order):
        self.daily_accum_cost += abs(order.cost)

    def finalize(self):
        # 턴오버(%) = (거래에 사용된 총 비용 / 평균 밸류) * 100
        self.turnover = self.calculate_turn_over()

    def calculate_turn_over(self):
        # 턴오버(%) = (거래에 사용된 총 비용 / 평균 밸류) * 100
        if self.daily_turnover_count == 0:
            return 0.0

        return (self.accum_turnover / self.daily_turnover_count) * 100
