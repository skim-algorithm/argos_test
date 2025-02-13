class Base:
    def __init__(self, order_handler):
        self.order_handler = order_handler
        self.current_year = 0
        self.ret = {}

    def on_data(self, df):
        """ 데이터프레임이 한 번 돌아갈 때마다 호출되는 함수 """
        pass

    def on_order_done(self, order):
        """ 주문이 완료되었을 때 호출되는 함수 """
        pass

    def finalize(self):
        """
        turnover와 같이 최종 한 번만 계산하는게 더 효율적인 정보의 경우 finalize에서 계산한다.
        """
        pass
