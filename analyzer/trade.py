from . import base
from common import enum


class Trade(base.Base):
    def __init__(self, symbol, order_handler):
        base.Base.__init__(self, order_handler)
        self.symbol = symbol

        self.number_of_open_orders = 0
        self.number_of_closed_orders = 0
        self.number_of_wins = 0
        self.number_of_loses = 0
        self.best_pnl = float("-inf")
        self.worst_pnl = float("inf")
        self.total_profit = 0
        self.total_loses = 0
        self.total_funding_fee = 0.0

        self.closed_orders = []

    def on_order_done(self, order):
        if order.opt is enum.OrderOpt.OPEN:
            self.number_of_open_orders += 1
        if order.opt is enum.OrderOpt.CLOSE:
            # self.number_of_open_orders -= 1
            self.number_of_closed_orders += 1

            net_pnl = order.pnl_w_comm - order.funding_fee

            if order.pnl_w_comm > 0:
                self.number_of_wins += 1
                self.total_profit += net_pnl
            else:
                self.number_of_loses += 1
                self.total_loses += net_pnl

            self.best_pnl = max(self.best_pnl, net_pnl)
            self.worst_pnl = min(self.worst_pnl, net_pnl)
            self.total_funding_fee += order.funding_fee

            order.returns = net_pnl / (self.order_handler.get_usd() - net_pnl) * 100

    def finalize(self):
        self.win_rate = self.calculate_win_rate()
        self.pnl_ratio = self.calculate_pnl_ratio()

    def calculate_win_rate(self):
        if self.number_of_closed_orders <= 0:
            return 0.0

        return self.number_of_wins / self.number_of_closed_orders * 100

    def calculate_pnl_ratio(self):
        pnl_ratio = 0.0
        win_ratio = 0.0
        lose_ratio = 0.0

        if self.number_of_wins > 0:
            win_ratio = self.total_profit / self.number_of_wins

        if self.number_of_loses > 0:
            lose_ratio = abs(self.total_loses / self.number_of_loses)

        if lose_ratio > 0.0:
            pnl_ratio = win_ratio / lose_ratio

        return pnl_ratio
