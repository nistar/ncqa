from datetime import datetime
from pandas import DatetimeIndex
from model.member import Member
from model.enrollment import Enrollment
from model.visit import Visit
from model.pharm import Pharm
from model.mmdf import MMDF


class Context:

    def __init__(
            self,
            run_date: datetime
    ):

        from collections import defaultdict
        from sortedcontainers import SortedList
        from db.mongo import connector

        self.run_date = run_date
        self.member: Member
        self.enrollments = SortedList([Enrollment])
        self.visits = SortedList([Visit])
        self.pharm = None
        self.mmdf: [MMDF] = []
        self.age_eligibility = False
        self.ce_eligibility = False
        self.enrolled_in_snp = False
        self.required_exclusion = False
        self.long_term_institution = False
        self.gaps_in_care = False
        self.frailty = False
        self.advanced_illness = False
        self.anchor_date_eligibility = False
        self.on_dementia_meds = False
        self.bilateral_mastectomy = False
        self.optional_exclusions = False
        self.overlapping_enrollments = defaultdict(list)
        self.db_conn = connector.Connector()

    def __repr__(self):
        return 'Run date {}'.format(self.run_date)

    def reset(self) -> None:
        self.enrollments.clear()
        self.overlapping_enrollments.clear()
        self.visits.clear()
        self.mmdf.clear()
        self.age_eligibility = False
        self.ce_eligibility = False
        self.enrolled_in_snp = False
        self.required_exclusion = False
        self.long_term_institution = False
        self.gaps_in_care = False
        self.frailty = False
        self.advanced_illness = False
        self.pharm = None
        self.on_dementia_meds = False
        self.bilateral_mastectomy = False
        self.optional_exclusions = False

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

        self.enrollments.add(Enrollment(enrolled_date_range, payer))

    def add_encounter(self, encounter: dict) -> None:
        service_date = datetime.fromisoformat(encounter['ServiceDate'])
        agg_codes = encounter['AggregatedCodes']
        self.visits.add(Visit(service_date, agg_codes, encounter['CptMod1']))

    def add_pharm(self, pharm: dict) -> None:
        service_date = datetime.fromisoformat(pharm['ServiceDate'])
        dispensed_med_code = pharm['NDCDrugCode']
        self.pharm = Pharm(service_date, dispensed_med_code, self.db_conn)

    def add_mmdf(self, mmdf: dict) -> None:
        run_date = datetime.fromisoformat(mmdf['Rundate'])
        lti_flag = mmdf['LongTermInstitutionalStatus']
        self.mmdf.append(MMDF(run_date, lti_flag))


