from pandas import DatetimeIndex


class Enrollment:
    def __init__(
            self,
            dates: DatetimeIndex,
            payer: str
    ):
        self.dates = dates
        self.payer = payer

    def __repr__(self):
        return 'From: {} To: {}. Payer {}'.format(self.dates[0], self.dates[-1], self.payer)

    def __lt__(self, other) -> bool:
        return self.dates[0] < other.dates[0]



