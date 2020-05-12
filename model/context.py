from datetime import datetime
from model.member import Member
from model.enrollment import Enrollment
from pandas import DatetimeIndex


class Context:
    def __init__(
            self,
            run_date: datetime
    ):
        from collections import defaultdict
        self.run_date = run_date
        self.member: Member
        self.enrollments: [Enrollment] = []
        self.overlapping_enrollments = defaultdict(list)

    def __repr__(self):
        return 'Run date {}'.format(self.run_date)

    def reset(self) -> None:
        self.enrollments.clear()
        self.overlapping_enrollments.clear()

    def add_enrollment(self, enrollment: dict) -> None:
        from pandas import date_range
        from model.overlapping_enrollments import OverlappingEnrollments

        start_date = enrollment['StartDate']
        if start_date == 'NaT':
            return

        finish_date = enrollment['FinishDate']
        enrolled_date_range = date_range(start_date, finish_date)
        payer = enrollment['Payer']

        idx = 0
        for mem_enrollment in self.enrollments:
            overlapping_dates: DatetimeIndex = mem_enrollment.dates.intersection(enrolled_date_range)
            if not overlapping_dates.empty:
                overlap_enrollments = OverlappingEnrollments([payer, mem_enrollment.payer], overlapping_dates)

                # do not store duplicates
                if len(self.overlapping_enrollments[idx]) > 0 and \
                        self.overlapping_enrollments[idx][-1] == overlap_enrollments:
                    continue

                self.overlapping_enrollments[idx].append(overlap_enrollments)
            idx += 1

        self.enrollments.append(Enrollment(enrolled_date_range, payer))

    def has_dual_enrollments(self):
        print('hi')

