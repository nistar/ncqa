from datetime import datetime
from util import natus_config
from db.mongo import connector
from model.context import Context
from model.visit import Visit
from model import optional_exclusions


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
        # self.codes = natus_util.measure_to_codes(config_section_name, connection)
        self.anchor_date = eval(config.read_value(config_section_name, 'anchor_date'))
        continuous_enrollment_config = eval(config.read_value(config_section_name, "continuous.enrollment"))
        self.measurement_year_date_range = eval(config.read_value(config_section_name, 'measurement_year_date_range'))
        self.prior_measurement_year_date_range = eval(config.read_value(
            config_section_name,
            'prior_measurement_year_date_range')
        )
        self.continuous_enrollment = natus_util.continuous_enrollment(continuous_enrollment_config)
        self.age_rule = config.read_value(config_section_name, 'age_rule')
        self.eligible_date_range = eval(config.read_value(config_section_name, 'date_range'))
        self.ce_date_ranges = eval(config.read_value(config_section_name, 'ce_date_ranges'))
        self.ce_allowable_gaps = eval(config.read_value(config_section_name, 'ce_allowable_gaps'))
        self.ce_year_to_idx = eval(config.read_value(config_section_name, 'ce_year_to_idx'))
        self.outpatient = connection.find('vs_name', 'Outpatient', 'vs_to_codes')['codes']
        # intersection with visit codes
        self.outpatient_codes = set([x['code'] for x in self.outpatient])
        self.observation = connection.find('vs_name', 'Observation', 'vs_to_codes')['codes']
        self.observation_codes = set([x['code'] for x in self.observation])
        self.frailty = connection.find('vs_name', 'Frailty', 'vs_to_codes')['codes']
        self.frailty_codes = [x['code'] for x in self.frailty]
        self.non_acute_inpatient = connection.find('vs_name', 'Nonacute Inpatient', 'vs_to_codes')['codes']
        self.non_acute_inpatient_codes = set([x['code'] for x in self.non_acute_inpatient])
        self.acute_inpatient = connection.find('vs_name', 'Acute Inpatient', 'vs_to_codes')['codes']
        self.acute_inpatient_codes = set([x['code'] for x in self.acute_inpatient])
        self.advanced_illness = connection.find('vs_name', 'Advanced Illness', 'vs_to_codes')['codes']
        self.advanced_illness_codes = set([x['code'] for x in self.advanced_illness])
        self.ed = connection.find('vs_name', 'ED', 'vs_to_codes')['codes']
        self.ed_codes = set([x['code'] for x in self.ed])
        self.bilateral_mastectomy = connection.find('vs_name', 'Bilateral Mastectomy', 'vs_to_codes')['codes']
        self.bilateral_mastectomy_codes = [x['code'] for x in self.bilateral_mastectomy]
        self.unilateral_mastectomy = connection.find('vs_name', 'Unilateral Mastectomy', 'vs_to_codes')['codes']
        self.unilateral_mastectomy_codes = [x['code'] for x in self.unilateral_mastectomy]
        self.bilateral_modifier = connection.find('vs_name', 'Bilateral Modifier', 'vs_to_codes')['codes']
        self.right_modifier = connection.find('vs_name', 'Right Modifier', 'vs_to_codes')['codes']
        self.left_modifier = connection.find('vs_name', 'Left Modifier', 'vs_to_codes')['codes']
        self.history_of_bilateral_mastectomy = connection.find('vs_name', 'History of Bilateral Mastectomy',
                                                               'vs_to_codes')['codes']
        self.history_of_bilateral_mastectomy_codes = [x['code'] for x in self.history_of_bilateral_mastectomy]
        self.absence_of_right_breast = connection.find('vs_name', 'Absence of Right Breast', 'vs_to_codes')['codes']
        self.absence_of_left_breast = connection.find('vs_name', 'Absence of Left Breast', 'vs_to_codes')['codes']
        self.unilateral_mastectomy_right = connection.find('vs_name', 'Unilateral Mastectomy Right',
                                                           'vs_to_codes')['codes']
        self.unilateral_mastectomy_right_codes = set([x['code'] for x in self.unilateral_mastectomy_right])
        self.unilateral_mastectomy_left = connection.find('vs_name', 'Unilateral Mastectomy Left',
                                                          'vs_to_codes')['codes']
        self.unilateral_mastectomy_left_codes = set([x['code'] for x in self.unilateral_mastectomy_left])

    def age_eligible(self, context: Context) -> None:
        context.age_eligibility = eval(self.age_rule)

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

    def enrollments(self, context: Context) -> None:

        accumulator = {}
        for enrollment in context.enrollments:

            # check anchor date
            if self.anchor_date in enrollment.dates:
                context.anchor_date_eligibility = True

            # check snp enrollment
            if not enrollment.dates.intersection(self.measurement_year_date_range).empty \
                    and enrollment.payer in ['SN1', 'SN2', 'SN3']:
                context.enrolled_in_snp = True

            if not enrollment.dates.intersection(self.eligible_date_range).empty:
                for ce_date_range in self.ce_date_ranges:
                    ce_intersection = ce_date_range.intersection(enrollment.dates)
                    if not ce_intersection.empty:
                        year = ce_intersection[0].year
                        if year in accumulator:
                            accumulator[year] = accumulator[year] & ce_intersection
                        else:
                            accumulator[year] = ce_intersection

        context.gaps_in_care = self.gaps_in_care(accumulator)

    @staticmethod
    def required_exclusions(context: Context) -> None:
        if context.member.age >= 66:
            if context.long_term_institution or context.enrolled_in_snp:
                context.required_exclusion = True
            elif context.frailty and (context.advanced_illness or context.on_dementia_meds):
                context.required_exclusion = True

    def mmdf(self, context: Context) -> None:
        for mmdf in context.mmdf:
            if mmdf.run_date in self.measurement_year_date_range and mmdf.lti_flag:
                context.long_term_institution = True

    def had_bilateral_mastectomy(
            self,
            visit: Visit,
            context: Context,
            optional_excl: optional_exclusions.OptionalExclusions
    ) -> None:
        visit_codes = set(visit.codes)

        if bool(visit_codes.intersection(self.bilateral_mastectomy_codes)):
            optional_excl.BilateralMastectomy.append(visit.service_date)

        if bool(visit_codes.intersection(self.history_of_bilateral_mastectomy_codes)):
            optional_excl.HistoryOfBilateralMastectomy.append(visit.service_date)

        # with left modifier
        if bool(visit_codes.intersection(self.unilateral_mastectomy_left_codes)):
            optional_excl.UnilateralMastectomyLeft.append(visit.service_date)

        # with right modifier
        if bool(visit_codes.intersection(self.unilateral_mastectomy_right_codes)):
            optional_excl.UnilateralMastectomyRight.append(visit.service_date)

        if bool(visit_codes.intersection([self.absence_of_left_breast[0]['code']])):
            optional_excl.AbsenceOfLeftBreast.append(visit.service_date)

        if bool(visit_codes.intersection([self.absence_of_right_breast[0]['code']])):
            optional_excl.AbsenceOfRightBreast.append(visit.service_date)

        if visit.modifier == self.bilateral_modifier[0]['code']:
            optional_excl.BilateralModifier.append(visit.service_date)

        if visit.modifier == self.left_modifier[0]['code']:
            optional_excl.LeftModifier.append(visit.service_date)

        if visit.modifier == self.right_modifier[0]['code']:
            optional_excl.RightModifier.append(visit.service_date)

        if bool(visit_codes.intersection(self.unilateral_mastectomy_codes)):
            optional_excl.UnilateralMastectomy.append(visit.service_date)

    def visit(self, context: Context) -> None:

        outpatient_visit_counter = 0
        inpatient_visit_counter = 0
        optional_excl = optional_exclusions.OptionalExclusions()

        for visit in context.visits:
            self.had_bilateral_mastectomy(visit, context, optional_excl)
            if visit.service_date.year == context.run_date.year \
                    and bool(set(visit.codes).intersection(self.frailty_codes)):
                context.frailty = True

            # TODO non acute encounter on different dates
            if visit.service_date.year in [context.run_date.year, context.run_date.year - 1]:
                if (bool(self.outpatient_codes.intersection(visit.codes)) or
                        bool(self.observation_codes.intersection(visit.codes)) or
                        bool(self.ed_codes.intersection(visit.codes)) or
                        bool(self.non_acute_inpatient_codes.intersection(visit.codes))):

                    if bool(self.advanced_illness_codes.intersection(visit.codes)):
                        outpatient_visit_counter += 1

                if (bool(self.acute_inpatient_codes.intersection(visit.codes)) and
                        bool(self.advanced_illness_codes.intersection(visit.codes))):

                    inpatient_visit_counter += 1

        if not optional_excl.empty():
            context.optional_exclusions = self.optional_exclusions(optional_excl)

        if outpatient_visit_counter >= 2 or inpatient_visit_counter >= 1:
            context.advanced_illness = True

    @staticmethod
    def optional_exclusions(opt_excl: optional_exclusions.OptionalExclusions) -> bool:
        if opt_excl.history_of_bilateral_mastectomy() or opt_excl.bilateral_mastectomy() or \
                opt_excl.unilateral_mastectomy_with_bilateral_mod() or \
                opt_excl.two_unilateral_mastectomies_without_mod() or \
                opt_excl.unilateral_mastectomy_without_mod_and_right_mastectomy() or \
                opt_excl.unilateral_mastectomy_without_mod_and_left_mastectomy() or \
                opt_excl.left_and_right_mastectomy():
            return True

        return False

    @staticmethod
    def dementia_meds(context: Context) -> None:
        if context.pharm:
            service_year = context.pharm.service_date.year
            if service_year in [context.run_date.year, context.run_date.year - 1]:
                for med_name in context.pharm.dispensed_med_name:
                    med_name_lower = med_name.lower()
                    if 'donepezil' in med_name_lower or 'galantamine' in med_name_lower or \
                            'rivastigmine' in med_name_lower or 'memantine' in med_name_lower:
                        context.on_dementia_meds = True

    def run(self, context: Context) -> None:
        self.age_eligible(context)
        if not context.age_eligibility:
            return

        self.enrollments(context)
        # no gaps in care
        if context.gaps_in_care:
            return
        if context.enrolled_in_snp:
            return
        if not context.anchor_date_eligibility:
            return

        self.mmdf(context)
        self.visit(context)
        self.dementia_meds(context)
        self.required_exclusions(context)

        if context.required_exclusion:
            return


if __name__ == '__main__':
    num = BCS(datetime(2018, 12, 31))
