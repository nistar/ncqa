import logging
import json
from datetime import datetime
from pandas import DatetimeIndex, date_range
from util import natus_logging, natus_config, natus_util
from db.mongo import connector
from dotmap import DotMap


class Filter:
    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_con = connector.Connector()
        self.member_filter = {}
#        self.load_filter()
        self.value_set_cache = {}

    def load_filter(self):
        import pickle
        with open(self.config.read_value('filter', 'input.filename'), 'rb') as input_file:
            self.member_filter = pickle.load(input_file)

    def lookup_value_set(
            self,
            value_set_name: str
    ) -> dict:

        if value_set_name not in self.value_set_cache:
            self.db_con.collection_name = 'valueset'
            self.value_set_cache[value_set_name] = self.db_con.find('ValueSetName', value_set_name)

        return self.value_set_cache[value_set_name]

    def age(self, context: DotMap) -> None:
        mem_id = context.Member.MemberId
        context.Member.Age = self.member_filter[mem_id]['Age']

    @staticmethod
    def enrollments(member: dict, context: DotMap) -> None:

        measurement_year_range = natus_util.measurement_year_range(
            context.RunDate, context.MemberEnrollmentDateRanges
        )

        for member_enrollment in member['enrollments']:
            # clean up missing dates
            if member_enrollment['StartDate'] == 'NaT':
                continue
            start_enrollment_date = datetime.fromisoformat(member_enrollment['StartDate'])
            end_enrollment_date = datetime.fromisoformat(member_enrollment['FinishDate'])
            payer = member_enrollment['Payer']
            current_enrollment_date_range = date_range(start_enrollment_date, end_enrollment_date)
            for continuous_enrollment_date_range in context.ContinuousEnrollmentDateRanges:
                idx = natus_util.measurement_year_index(
                    measurement_year_range,
                    continuous_enrollment_date_range.DateRange[0].year)
                date_intersection = current_enrollment_date_range.intersection(
                        continuous_enrollment_date_range.DateRange)
                if not date_intersection.empty:
                    context.MemberEnrollmentDateRanges[idx].DateRanges.append(date_intersection)
                    context.MemberEnrollmentDateRanges[idx].Payers.append(payer)

    def optional_exclusions(
            self,
            visit: dict,
            service_date: datetime,
            context: DotMap
    ) -> None:
        member_encounter_codes = visit['AggregatedCodes']
        bilateral_mastectomy_value_set = set(self.lookup_value_set('Bilateral Mastectomy')['Codes'])

        if bilateral_mastectomy_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.BilateralMastectomy = True

        unilateral_mastectomy_value_set = set(self.lookup_value_set('Unilateral Mastectomy')['Codes'])
        if unilateral_mastectomy_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.UnilateralMastectomy.append(service_date)

        bilateral_modifier_value_set = self.lookup_value_set('Bilateral Modifier')['Codes']
        if bilateral_modifier_value_set[0] == visit['CptMod1']:
            context.OptionalExclusions.BilateralModifier = True

        history_of_bilateral_mastectomy_value_set = set(self.lookup_value_set('History of Bilateral Mastectomy')['Codes'])
        if history_of_bilateral_mastectomy_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.HistoryOfBilateralMastectomy = True

        right_modifier_value_set = self.lookup_value_set('Right Modifier')['Codes']
        if right_modifier_value_set[0] == visit['CptMod1']:
            context.OptionalExclusions.RightModifier.append(service_date)

        left_modifier_value_set = self.lookup_value_set('Left Modifier')['Codes']
        if left_modifier_value_set[0] == visit['CptMod1']:
            context.OptionalExclusions.LeftModifier.append(service_date)

        absence_of_right_breast_value_set = set(self.lookup_value_set('Absence of Right Breast')['Codes'])
        if absence_of_right_breast_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.AbsenceOfRightBreast = True

        absence_of_left_breast_value_set = set(self.lookup_value_set('Absence of Left Breast')['Codes'])
        if absence_of_left_breast_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.AbsenceOfLeftBreast = True

        unilateral_mastectomy_right_value_set = set(self.lookup_value_set('Unilateral Mastectomy Right')['Codes'])
        if unilateral_mastectomy_right_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.UnilateralMastectomyRight.append(service_date)

        unilateral_mastectomy_left_value_set = set(self.lookup_value_set('Unilateral Mastectomy Left')['Codes'])
        if unilateral_mastectomy_left_value_set.intersection(member_encounter_codes):
            context.OptionalExclusions.UnilateralMastectomyLeft.append(service_date)

    def visits(self,
               member: dict,
               context: DotMap
               ) -> None:
        # init years
        encounter_year_range = natus_util.measurement_year_range(context.RunDate, context.Visits)

        if 'visits' in member:
            for visit in member['visits']:
                if visit['ServiceDate'] is None:
                    continue
                service_date = datetime.fromisoformat(visit['ServiceDate'])
                self.optional_exclusions(visit, service_date, context)
                if service_date.year in encounter_year_range:
                    idx = natus_util.measurement_year_index(encounter_year_range, service_date.year)
                    context.Visits[idx].Encounters.append(
                        {"ServiceDate": service_date,
                         "CodeSet": visit['AggregatedCodes']}
                    )

    @staticmethod
    def pharm(member: dict, context: DotMap) -> None:
        if 'pharm' in member:
            year_range = natus_util.measurement_year_range(context.RunDate, context.Pharm.MeasurementYears)
            pharm = member['pharm']
            dispensed_drug_year = datetime.fromisoformat(pharm['ServiceDate']).year
            if dispensed_drug_year in year_range:
                idx = natus_util.measurement_year_index(year_range, dispensed_drug_year)
                context.Pharm.MeasurementYears[idx].DispensedMedication = pharm['NDCDrugCode']

    @staticmethod
    def mmdf(member: dict, context: DotMap) -> None:

        if 'HospiceLti' in member:
            member_hospice_ltis = member['HospiceLti']
            for member_hospice_lti in member_hospice_ltis:
                hospice_lti_run_date = datetime.fromisoformat(member_hospice_lti['Rundate'])
                if hospice_lti_run_date.year == context.RunDate.year:
                    lti_flag = member_hospice_lti['LongTermInstitutionalStatus']
                    if lti_flag:
                        context.MMDF.LTIFlag = True

    def pre_populate_context(
            self,
            member: dict,
            context: DotMap
    ):
        self.age(context)
        self.enrollments(member, context)
        self.visits(member, context)
        self.pharm(member, context)
        self.mmdf(member, context)

    @staticmethod
    def member_age(
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> int:
        result = natus_util.json_path_query(exclusion_context.Age.Path, pre_processed_context)
        for match in result:
            return match.value

    def age_exclusion_rule(
            self,
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> None:
        age = self.member_age(pre_processed_context, exclusion_context)
        gte = exclusion_context.Age.GTE
        lte = exclusion_context.Age.LTE
        if gte <= age <= lte:
            exclusion_context.Age.Excluded = False

    def continuous_enrollment_exclusion_rule(
            self,
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> None:
        member_enrollment_date_ranges = natus_util.json_path_query(
            exclusion_context.ContinuousEnrollment.MemberEnrollmentDateRangePath,
            pre_processed_context)
        continuous_enrollment_date_ranges = natus_util.json_path_query(
            exclusion_context.ContinuousEnrollment.ContinuousEnrollmentDateRangePath,
            pre_processed_context)
        age = self.member_age(pre_processed_context, exclusion_context)
        idx = 0
        anchor_date = self.anchor_date_exclusion_rule(pre_processed_context, exclusion_context)
        snp = exclusion_context.SNP.Payers
        member_enrollments_printable_version = 'Member coverage for ' + pre_processed_context['Member']['MemberId'] \
                                               + ' '
        for member_enrollment_date_range in member_enrollment_date_ranges:
            current_year_date_range = continuous_enrollment_date_ranges[idx].value['DateRange']
            # accumulate member enrollments within same year
            member_enrollments_accumulator = None
            for date_range_in_same_year in member_enrollment_date_range.value['DateRanges']:
                member_enrollments_printable_version += 'Begin: ' + str(date_range_in_same_year[0]) + ' End: ' + \
                    str(date_range_in_same_year[len(date_range_in_same_year) - 1]) + '# '
                if member_enrollments_accumulator is None:
                    member_enrollments_accumulator = date_range_in_same_year
                else:
                    member_enrollments_accumulator = member_enrollments_accumulator.append(date_range_in_same_year)

            for current_member_payer in member_enrollment_date_range.value['Payers']:
                if current_member_payer in snp and age >= exclusion_context.SNP.gte.Age:
                    exclusion_context.SNP.Excluded = True

                # piggyback anchor date check
                if anchor_date in member_enrollments_accumulator:
                    exclusion_context.AnchorDate.Excluded = False

                diff = current_year_date_range.difference(member_enrollments_accumulator)
                if not diff.empty:
                    gte = exclusion_context.ContinuousEnrollment.GapsInCare[idx].GTE
                    if diff.size >= gte:
                        exclusion_context.ContinuousEnrollment.Excluded = True
            idx += 1
        print(member_enrollments_printable_version)

    @staticmethod
    def anchor_date_exclusion_rule(
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> datetime:
        return datetime(
            pre_processed_context['RunDate'].year,
            exclusion_context.AnchorDate.Month,
            exclusion_context.AnchorDate.Day
        )

    @staticmethod
    def is_enrolled_in_snp(
            pre_processed_context: dict
    ) -> bool:
        for member_enrollment_date_range in pre_processed_context['MemberEnrollmentDateRanges']:
            for enrolled_time_period in member_enrollment_date_range['DateRanges']:
                if pre_processed_context['RunDate'].year == enrolled_time_period[0].year:
                    if set(member_enrollment_date_range['Payers']).intersection(["SN1", "SN2", "SN3"]):
                        return True
        return False

    @staticmethod
    def is_long_term_institution(
            pre_processed_context: dict
    ) -> bool:
        if 'MMDF' in pre_processed_context and 'LTIFlag' in pre_processed_context['MMDF']:
            if pre_processed_context['MMDF']['LTIFlag']:
                return True
        return False

    def is_frailty(
            self,
            member_codes: list
    ) -> bool:
        value_set_codes = self.lookup_value_set('Frailty')['Codes']
        if set(member_codes).intersection(value_set_codes):
            return True
        return False

    def is_advanced_illness(
            self,
            visits: list
    ) -> bool:
        outpatient_value_set = self.lookup_value_set('Outpatient')['Codes']
        observation_value_set = self.lookup_value_set('Observation')['Codes']
        non_acute_inpatient_value_set = self.lookup_value_set('Nonacute Inpatient')['Codes']
        acute_inpatient_value_set = self.lookup_value_set('Acute Inpatient')['Codes']
        advanced_illness_value_set = self.lookup_value_set('Advanced Illness')['Codes']
        ed_value_set = self.lookup_value_set('ED')['Codes']
        outpatient_visit_counter = 0
        acute_inpatient_encounter_counter = 0
        has_advanced_illness = False
        service_date = None

        for member_visit in visits:
            for member_encounter in member_visit['Encounters']:
                member_codes = set(member_encounter['CodeSet'])
                if member_codes.intersection(advanced_illness_value_set):
                    has_advanced_illness = True
                if member_codes.intersection(acute_inpatient_value_set):
                    acute_inpatient_encounter_counter += 1
                if member_codes.intersection(outpatient_value_set) or \
                        member_codes.intersection(observation_value_set) or \
                        member_codes.intersection(ed_value_set) or \
                        member_codes.intersection(non_acute_inpatient_value_set):
                    if service_date is None or service_date != member_encounter['ServiceDate']:
                        service_date = member_encounter['ServiceDate']
                        outpatient_visit_counter += 1

        if has_advanced_illness:
            if outpatient_visit_counter >= 2 or acute_inpatient_encounter_counter >= 1:
                return True
        return False

    def on_dementia_medications(
            self,
            member_pharm: dict
    ) -> bool:
        import re
        self.db_con.collection_name = 'ndc'
        dementia_medications = ['donepezil', 'galantamine', 'rivastigmine', 'memantine']
        for pharm_measurement_year in member_pharm['MeasurementYears']:
            if pharm_measurement_year['DispensedMedication'] is not None:
                ndc_info = self.db_con.find('NDC', pharm_measurement_year['DispensedMedication'][0:9])
                if ndc_info is not None:
                    ndc_code = ' '.join(ndc_info['DrugNames'])
                    for dementia_medication in dementia_medications:
                        if re.match(dementia_medication, ndc_code, re.IGNORECASE):
                            return True
        return False

    def is_frailty_and_advanced_illness(
            self,
            pre_processed_context: dict
    ) -> bool:
        run_date_year = pre_processed_context['RunDate'].year
        for member_visit in pre_processed_context['Visits']:
            if len(member_visit['Encounters']) > 0:
                for encounter in member_visit['Encounters']:
                    if encounter['ServiceDate'].year == run_date_year:
                        if self.is_frailty(encounter['CodeSet']):
                            if self.is_advanced_illness(pre_processed_context['Visits']) or \
                                    self.on_dementia_medications(pre_processed_context['Pharm']):
                                return True
        return False

    def visit_exclusion_rule(
            self,
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> None:

        if pre_processed_context['Member']['Age'] >= 66:
            if self.is_enrolled_in_snp(pre_processed_context):
                exclusion_context.SNP.Excluded = True
            if self.is_long_term_institution(pre_processed_context):
                exclusion_context.LTI.Excluded = True
            if self.is_frailty_and_advanced_illness(pre_processed_context):
                exclusion_context.FrailtyAdvancedIllness = True

    def exclusion_rules(
            self,
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> None:
        self.age_exclusion_rule(pre_processed_context, exclusion_context)
        self.continuous_enrollment_exclusion_rule(pre_processed_context, exclusion_context)
        self.visit_exclusion_rule(pre_processed_context, exclusion_context)

    def process_exclusions(
            self,
            member: dict,
            context: DotMap
    ):
        from pprint import pprint

        self.pre_populate_context(member, context)
        exclusion_context = DotMap(json.loads(self.config.read_value('bcs', 'exclusion.rule')))
        self.exclusion_rules(context.toDict(), exclusion_context)
        self.optional_exclusions_rules(context.toDict(), exclusion_context)
        pprint(exclusion_context.OptionalExclusions.toDict())

    @staticmethod
    def is_14_or_more_days_apart(
            service_dates: list
    ) -> bool:
        service_dates.sort()
        previous_service_date = None
        for service_date in service_dates:
            if previous_service_date is not None:
                if (service_date - previous_service_date).days >= 14:
                    return True
            previous_service_date = service_date

        return False

    def left_mastectomy(
            self,
            left_mastectomy_descriptor: dict
    ) -> bool:
        if (left_mastectomy_descriptor['LeftModifier'] and
            self.same_visit(left_mastectomy_descriptor['UnilateralMastectomy'],
                            left_mastectomy_descriptor['LeftModifier'])) or \
                left_mastectomy_descriptor['UnilateralMastectomyLeft']:
            return True

        return False

    def right_mastectomy(
            self,
            right_mastectomy_descriptor: dict
    ) -> bool:
        if (right_mastectomy_descriptor['RightModifier'] and
            self.same_visit(right_mastectomy_descriptor['UnilateralMastectomy'],
                            right_mastectomy_descriptor['RightModifier'])) or \
                right_mastectomy_descriptor['UnilateralMastectomyRight']:
            return True

        return False

    @staticmethod
    def same_visit(
            dates_1: [datetime],
            dates_2: [datetime]
    ) -> bool:
        # make sure assumptions are correct

        if len(dates_1) == len(dates_2):
            return dates_1 == dates_2
        else:
            raise ValueError

    def optional_exclusions_rules(
            self,
            pre_processed_context: dict,
            exclusion_context: DotMap
    ) -> None:
        if pre_processed_context['OptionalExclusions']['BilateralMastectomy'] or \
                pre_processed_context['OptionalExclusions']['HistoryOfBilateralMastectomy']:
            exclusion_context.OptionalExclusions.Excluded = True
            exclusion_context.OptionalExclusions.Reason = 'Bilateral Mastectomy or History of ' + \
                'Bilateral Mastectomy'
            return

        if pre_processed_context['OptionalExclusions']['UnilateralMastectomy']:
            if pre_processed_context['OptionalExclusions']['BilateralModifier']:
                exclusion_context.OptionalExclusions.Excluded = True
                exclusion_context.OptionalExclusions.Reason = 'Unilateral Mastectomy with Bilateral Modifier'
                return

            unilateral_mastectomy_dates = pre_processed_context['OptionalExclusions']['UnilateralMastectomy']
            if len(unilateral_mastectomy_dates) >= 2 and \
                    self.is_14_or_more_days_apart(unilateral_mastectomy_dates):
                exclusion_context.OptionalExclusions.Excluded = True
                exclusion_context.OptionalExclusions.Reason = 'Unilateral Mastectomy with Visits 14 or More Days Apart'
                return

            if self.left_mastectomy({
                'UnilateralMastectomy': pre_processed_context['OptionalExclusions']['UnilateralMastectomy'],
                'LeftModifier': pre_processed_context['OptionalExclusions']['LeftModifier'],
                'UnilateralMastectomyLeft': pre_processed_context['OptionalExclusions']['UnilateralMastectomyLeft']
            }):
                exclusion_context.OptionalExclusions.Excluded = True
                exclusion_context.OptionalExclusions.Reason = 'Unilateral with Left Mastectomy'
                return

            if self.right_mastectomy({
                'UnilateralMastectomy': pre_processed_context['OptionalExclusions']['UnilateralMastectomy'],
                'RightModifier': pre_processed_context['OptionalExclusions']['RightModifier'],
                'UnilateralMastectomyRight': pre_processed_context['OptionalExclusions']['UnilateralMastectomyRight']
            }):
                exclusion_context.OptionalExclusions.Excluded = True
                exclusion_context.OptionalExclusions.Reason = 'Unilateral with Right Mastectomy'
                return

            if pre_processed_context['OptionalExclusions']['AbsenceOfRightBreast'] and \
                    pre_processed_context['OptionalExclusions']['AbsenceOfLeftBreast']:
                exclusion_context.OptionalExclusions.Excluded = True
                exclusion_context.OptionalExclusions.Reason = 'Absence of Right and Left Breast'
                return


if __name__ == '__main__':
    f = Filter()
