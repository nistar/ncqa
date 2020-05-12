from pandas import DatetimeIndex


class OverlappingEnrollments:
    def __init__(
            self,
            payers: list,
            overlapping_dates: DatetimeIndex
    ):
        self.payers = payers
        self.overlapping_dates = overlapping_dates
        self.dual_enrolled = self.is_dual_enrolled()

    def __eq__(self, other):
        if self.overlapping_dates.size != other.overlapping_dates.size:
            return False
        return self.payers == other.payers

    def __str__(self):
        return str(self.payers) + ' From: ' + self.overlapping_dates[0].strftime('%Y-%m-%d') + \
               ' To: ' + self.overlapping_dates[-1].strftime('%Y-%m-%d') + '. Dual enrollment: ' + \
               str(self.dual_enrolled)

    def is_dual_enrolled(self) -> bool:
        if self.payers[0] == self.payers[1]:
            return True
        return False

