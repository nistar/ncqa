import logging
from datetime import datetime
from util import natus_logging, natus_config
import json
from pandas import DatetimeIndex
from model.enrollment import Enrollment
from db.mongo import connector
from natusfilter import exclude_members
from model.context import Context


class Analyzer:

    def __init__(
            self,
            run_date: datetime
    ):
        config = natus_config.NATUSConfig('ncqa')
        config_section_name = self.__class__.__name__
        self.log = natus_logging.NATUSLogging(config_section_name, logging.INFO)
        self.run_date = run_date
        measures = json.loads(config.read_value(config_section_name, 'measures'))
        self.db_con = connector.Connector()
        self.measure_pipeline = self.init_measures(measures)

    def init_measures(
            self,
            measures: list
    ) -> list:
        from importlib import import_module
        """
        Create a pipeline of measures scanned from dir
        """
        measure_pipeline = []
        for measure in measures:
            mod = import_module('measures.' + measure.lower())
            # init measure
            Klass = getattr(mod, measure)
            measure_pipeline.append(Klass(self.run_date, self.db_con))

        return measure_pipeline

    @staticmethod
    def init_member(
            member: dict,
            context: Context
    ) -> None:
        from model.member import Member
        from util import natus_util

        context.reset()
        member_id = member['MemberId']
        age = natus_util.age(member['DOB'], context.run_date)
        is_female = True if member['Gender'] == 'F' else False
        context.member = Member(member_id, age, is_female)

    @staticmethod
    def init_enrollments(
            member: dict,
            context: Context
    ):

        member_enrollments = member['enrollments']
        for member_enrollment in member_enrollments:
            context.add_enrollment(member_enrollment)

    @staticmethod
    def init_visits(
            member: dict,
            context: Context
    ) -> None:

        if 'visits' in member:
            for visit in member['visits']:
                if visit['ServiceDate'] is None:
                    continue
                context.add_encounter(visit)

    @staticmethod
    def init_pharm(
            member: dict,
            context: Context
    ) -> None:
        if 'pharm' in member:
            pharm = member['pharm']
            context.add_pharm(pharm)

    @staticmethod
    def init_mmdf(
            member: dict,
            context: Context
    ) -> None:
        if 'HospiceLti' in member:
            mmdfs = member['HospiceLti']
            for mmdf in mmdfs:
                context.add_mmdf(mmdf)

    @staticmethod
    def dates_overlap(enrollments: [Enrollment]):

        enrollment_count = len(enrollments)
        overlap_count = 0
        enrollments_display = []
        if enrollment_count > 2:
            for i in range(0, enrollment_count):
                enr = enrollments[i]
                for j in range(i + 1, enrollment_count):
                    enr_2 = enrollments[j]
                    intersect = enr.dates_enrolled.intersection(enr_2.dates_enrolled)
                    if not intersect.empty:
                        enrollments_display.append({
                            'Payer1': enr.payer,
                            'Payer2': enr_2.payer,
                            'From': intersect[0].strftime('%Y-%m-%d'),
                            'To': intersect[-1].strftime('%Y-%m-%d')
                        })
                        overlap_count += 1

        if overlap_count > 2:
            print(enrollments_display)

    def process_member(
            self,
            member: dict,
            context: Context
    ) -> None:
        # init context

        self.init_member(member, context)
        self.init_enrollments(member, context)
        self.init_visits(member, context)
        self.init_pharm(member, context)
        self.init_mmdf(member, context)

        for measure in self.measure_pipeline:
            measure.run(context)

    def start(self, member_id: str):
        ctx = Context(self.run_date)
        if member_id:
            self.process_member(self.db_con.find('MemberId', member_id, 'member'), ctx)
        else:
            for member in self.db_con.read('member'):
                self.process_member(member, ctx)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
       description="This script will analyze each member, excluding those who don't meet eligibility criteria"
    )

    parser.add_argument(
        '-m',
        '--memberid',
        metavar='',
        type=str,
        help='Member ID'
    )

    parser.add_argument(
        '-r',
        '--rundate',
        metavar='',
        type=str,
        help='Measurement date'
    )
    args = parser.parse_args()

    a = Analyzer(datetime.strptime(args.rundate, '%Y%m%d') if args.rundate else datetime.now())

    a.start(args.memberid)
