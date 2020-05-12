from json import loads
import logging
import pandas as pd
from pandas import DatetimeIndex
from datetime import datetime
from json import dumps
from util import natus_config, natus_logging


class AssembledMember:

    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.merged_member_filename = self.config.read_value('merge', 'merged.member.filename')
        self.members_input_filename = self.config.read_value('merge', 'members.input.filename')
        self.filter_output_filename = self.config.read_value('merge', 'filter.output.filename')
        self.run_date = datetime.now()
        self.enrollments = {}
        self.visits = {}
        self.providers = {}
        self.pharm = {}
        self.mmdf = {}

    def read_member_en(self):
        self.log.info('Reading enrollments')
        with open(self.config.read_value('merge', 'enrollments.input.filename')) as input_file:
            for line in input_file:
                enrollment_json = loads(line)
                mem_id = enrollment_json['MemberId']
                self.enrollments[mem_id] = enrollment_json['enrollments']

    def read_visits(self):
        self.log.info('Reading visits')
        with open(self.config.read_value('merge', 'visits.input.filename')) as input_file:
            for line in input_file:
                visit_json = loads(line)
                mem_id = visit_json['MemberId']
                self.visits[mem_id] = visit_json['visits']

    def read_providers(self):
        self.log.info('Reading providers')
        with open(self.config.read_value('merge', 'providers.input.filename')) as input_file:
            for line in input_file:
                provider_json = loads(line)
                provider_id = provider_json['ProviderId']
                del provider_json['ProviderId']
                self.providers[provider_id] = provider_json

    def read_pharm(self):
        self.log.info('Reading pharmacy')
        with open(self.config.read_value('merge', 'pharm.input.filename')) as input_file:
            for line in input_file:
                pharm_json = loads(line)
                mem_id = pharm_json['MemberId']
                del pharm_json['MemberId']
                self.pharm[mem_id] = pharm_json

    def read_mmdf(self):
        self.log.info('Reading mmdf')
        with open(self.config.read_value('merge', 'mmdf.input.filename')) as input_file:
            for line in input_file:
                mmdf_json = loads(line)
                mem_id = mmdf_json['MemberId']
                del mmdf_json['MemberId']
                self.mmdf[mem_id] = mmdf_json['HospiceLti']

    def datetime_to_age(self, member_json) -> int:
        dob = member_json['DOB']
        dob_dt = datetime.fromisoformat(dob)
        return self.run_date.year - dob_dt.year - \
            ((self.run_date.month, self.run_date.day) < (dob_dt.month, dob_dt.day))

    def current_product_line(self, member_json) -> str:
        enrollments = member_json['enrollments']
        for enrollment in enrollments:
            product_line = enrollment['Payer']
            try:
                start_date = datetime.fromisoformat(enrollment['StartDate'])
                finish_date = datetime.fromisoformat(enrollment['FinishDate'])
                if start_date <= self.run_date <= finish_date:
                    return product_line
            except ValueError:
                continue

    def continuous_enrollment(self) -> DatetimeIndex:
        from dateutil import relativedelta

        end_of_measurement_year = datetime(self.run_date.year, month=12, day=31)
        continuous_enrollment_months = self.config.read_value('merge', 'bcs.continuous.enrollment.months')
        start_of_continuous_enrollment = end_of_measurement_year - \
            relativedelta.relativedelta(months=int(continuous_enrollment_months))
        # reset to beginning of month
        start_of_continuous_enrollment = datetime(start_of_continuous_enrollment.year,
                                                  start_of_continuous_enrollment.month, 1)
        return pd.date_range(start_of_continuous_enrollment, end_of_measurement_year)

    def generate_filter_input(self, filter_input):
        import pickle

        with open(self.filter_output_filename, 'wb') as output_file:
            pickle.dump(filter_input, output_file)

    def merge_member_data(self):
        """
        This method combines several inputs to generate complete member info to be
        loaded into DB. Also, it will create natusfilter data to avoid extra processing
        at later stage using proprietary python pickle mechanism

        :return:
        """
        self.read_member_en()
        self.read_visits()
        self.read_providers()
        self.read_pharm()
        self.read_mmdf()

        self.log.info('Reading member GM')
        # Member assembly is GM driven
        filter_input_data = {}
        with open(self.members_input_filename) as input_file, \
                open(self.merged_member_filename, 'w') as output_file:
            for line in input_file:
                member_json = loads(line)
                mem_id = member_json['MemberId']
                if mem_id in self.enrollments:
                    member_json['enrollments'] = self.enrollments[mem_id]

                if mem_id in self.pharm:
                    member_json['pharm'] = self.pharm[mem_id]

                if mem_id in self.mmdf:
                    member_json['HospiceLti'] = self.mmdf[mem_id]

                if mem_id in self.visits:
                    patient_visits: list = self.visits[mem_id]
                    for patient_visit in patient_visits:
                        prov_id = patient_visit['ProviderId']
                        if prov_id in self.providers:
                            patient_visit['provider'] = self.providers[prov_id]

                    member_json['visits'] = patient_visits

                mem_filter = {
                    'Age': self.datetime_to_age(member_json),
                    'ProductLine': self.current_product_line(member_json),
                    'ContinuousEnrollment': self.continuous_enrollment()
                }

                filter_input_data[mem_id] = mem_filter
                # pre compute certain fields for the natusfilter stage
                output_file.write(dumps(member_json) + '\n')

            self.generate_filter_input(filter_input_data)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
       description='This script will prepare member info for processing by the next stage in '
       'the pipeline. It will create fields such as - age and gaps in care - that can be fed '
       'directly into the natusfilter, as well as preparing member file ready to be loaded into db.'
    )

    parser.add_argument(
        '-r',
        '--rundate',
        metavar='',
        type=str,
        help='Run date in the YYYYMMDD format. If not specified current date is assumed'
    )
    args = parser.parse_args()

    member = AssembledMember()
    if args.rundate:
        member.run_date = datetime.strptime(args.rundate, '%Y%m%d')

    member.merge_member_data()
