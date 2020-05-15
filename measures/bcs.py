from datetime import datetime
from util import natus_config
from db.mongo import connector
from model.context import Context


class BCS:

    def __init__(
            self,
            run_date: datetime,
            connection: connector.Connector
    ):
        from util import natus_util
        from pandas import date_range

        config = natus_config.NATUSConfig('ncqa')
        # shorthand notation
        config_section_name = self.__class__.__name__
        self.codes = natus_util.measure_to_codes(config_section_name, connection)
        self.anchor_date = eval(config.read_value(config_section_name, 'anchor.date'))
        continuous_enrollment_config = eval(config.read_value(config_section_name, "continuous.enrollment"))
        self.continuous_enrollment = natus_util.continuous_enrollment(continuous_enrollment_config)
        self.age_rule = config.read_value(config_section_name, 'age_rule')
        self.eligible_date_range = eval(config.read_value(config_section_name, 'date_range'))
        self.ce_date_ranges = eval(config.read_value(config_section_name, 'ce_date_ranges'))
        self.ce_allowable_gaps = eval(config.read_value(config_section_name, 'ce_allowable_gaps'))
        self.ce_year_to_idx = eval(config.read_value(config_section_name, 'ce_year_to_idx'))

    def age_eligible(self, context: Context) -> bool:
        return eval(self.age_rule)

    def gaps_in_care(self, member_enrollments: dict) -> bool:
        for year, enrollment_dates in member_enrollments.items():
            valid_year_date_range = self.ce_date_ranges[self.ce_year_to_idx[year]]
            # shortcut
            if valid_year_date_range.size == enrollment_dates.size:
                continue

            for idx in range(0, valid_year_date_range.size):
                # when coverage stops before year end
                if idx >= enrollment_dates.size:
                    return self.ce_allowable_gaps[year] < valid_year_date_range.size - \
                           enrollment_dates.size

                ce_date = valid_year_date_range[idx]
                diff_in_days = (enrollment_dates[idx] - ce_date).days

                if diff_in_days > self.ce_allowable_gaps[year]:
                    return True
        return False

    def continuous_enrollment_eligible(self, context: Context) -> bool:

        accumulator = {}
        for enrollment in context.enrollments:
            if not enrollment.dates.intersection(self.eligible_date_range).empty:
                for ce_date_range in self.ce_date_ranges:
                    ce_intersection = ce_date_range.intersection(enrollment.dates)
                    if not ce_intersection.empty:
                        year = ce_intersection[0].year
                        if year in accumulator:
                            accumulator[year] = accumulator[year] & ce_intersection
                        else:
                            accumulator[year] = ce_intersection

        return self.gaps_in_care(accumulator)

    def run(self, member: dict, context: Context) -> None:
        context.age_eligibility = self.age_eligible(context)
        if not context.age_eligibility:
            return

        context.ce_eligibility = self.continuous_enrollment_eligible(context)


if __name__ == '__main__':
    num = BCS(datetime(2018, 12, 31))
