import pandas as pd
import numpy as np
from json import dumps
from util.natus_config import NATUSConfig
import logging


class MemberEN:

    def __init__(self):
        self.config = NATUSConfig('ncqa')
        self.log = logging.getLogger(self.__class__.__name__)
        self.enrollments = {}

    def read_member_en(self):
        col_breaks = [(0, 16),
                      (16, 24),
                      (24, 32),
                      (32, 33),
                      (33, 34),
                      (34, 35),
                      (35, 36),
                      (36, 37),
                      (37, 38),
                      (38, 39),
                      (39, 40),
                      (40, 43),
                      (43, 44),
                      (44, 54)]
        col_names = ['MemberId',
                     'StartDate',
                     'FinishDate',
                     'Dental',
                     'Drug',
                     'MentalHealthInpatient',
                     'MentalHealthIntensiveOutpatient',
                     'MentalHealthOutpatientED',
                     'ChemDepInpatient',
                     'ChemDepIntensiveOutpatient',
                     'ChemDepOutpatientED',
                     'Payer',
                     'HealthPlanEmployeeFlag',
                     'Indicator']
        col_types = {'MemberId': np.unicode,
                     'StartDate': np.object,
                     'FinishDate': np.object,
                     'Dental': np.unicode,
                     'Drug': np.unicode,
                     'MentalHealthInpatient': np.unicode,
                     'MentalHealthIntensiveOutpatient': np.unicode,
                     'MentalHealthOutpatientED': np.unicode,
                     'ChemDepInpatient': np.unicode,
                     'ChemDepIntensiveOutpatient': np.unicode,
                     'ChemDepOutpatientED': np.unicode,
                     'Payer': np.unicode,
                     'HealthPlanEmployeeFlag': np.unicode,
                     'Indicator': np.unicode}
    # Indicator is used for VBP4P. It's always 'A' for HEDIS
        self.log.info('Reading input file')
        df = pd.read_fwf(
            self.config.read_value('setup', 'member-en.input.file.name'),
            colspecs=col_breaks,
            names=col_names,
            dtype=col_types,
            parse_dates=[1, 2]
        )

        for index, rows in df.iterrows():
            enrollment = {
                'StartDate': rows.StartDate.to_pydatetime().isoformat(),
                'FinishDate': rows.FinishDate.to_pydatetime().isoformat()}
            if rows.Dental == 'Y':
                enrollment['Dental'] = True
            else:
                enrollment['Dental'] = False

            if rows.Drug == 'Y':
                enrollment['Drug'] = True
            else:
                enrollment['Drug'] = False

            if rows.MentalHealthInpatient == 'Y':
                enrollment['MentalHealthInpatient'] = True
            else:
                enrollment['MentalHealthInpatient'] = False

            if rows.MentalHealthIntensiveOutpatient == 'Y':
                enrollment['MentalHealthIntensiveOutpatient'] = True
            else:
                enrollment['MentalHealthIntensiveOutpatient'] = False

            if rows.MentalHealthOutpatientED == 'Y':
                enrollment['MentalHealthOutpatientED'] = True
            else:
                enrollment['MentalHealthOutpatientED'] = False

            if rows.ChemDepInpatient == 'Y':
                enrollment['ChemDepInpatient'] = True
            else:
                enrollment['ChemDepInpatient'] = False

            if rows.ChemDepIntensiveOutpatient == 'Y':
                enrollment['ChemDepIntensiveOutpatient'] = True
            else:
                enrollment['ChemDepIntensiveOutpatient'] = False

            if rows.ChemDepOutpatientED == 'Y':
                enrollment['ChemDepOutpatientED'] = True
            else:
                enrollment['ChemDepOutpatientED'] = False

            enrollment['Payer'] = rows.Payer
            enrollment['HealthPlanEmployeeFlag'] = rows.HealthPlanEmployeeFlag
            enrollment['Indicator'] = rows.Indicator

            if rows.MemberId not in self.enrollments:
                self.enrollments[rows.MemberId] = [enrollment]
            else:
                self.enrollments[rows.MemberId].append(enrollment)

    def write_member_en(self):
        with open(self.config.read_value('setup', 'member-en.output.file.name'), 'w') as output_file:
            for (key, value) in self.enrollments.items():
                enrollment_with_key = {
                    'MemberId': key,
                    'enrollments': value
                }
                output_file.write(dumps(enrollment_with_key) + '\n')


if __name__ == '__main__':
    member_en = MemberEN()
    member_en.read_member_en()
    member_en.write_member_en()

