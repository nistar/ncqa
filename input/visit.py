import pandas as pd
import numpy as np
import logging
from util import natus_config, natus_logging
from json import dumps


class Visit:

    def __init__(self):
        self.config = natus_config.NATUSConfig('ncqa')
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.visits = {}

    def process_visits(self) -> None:
        col_breaks = [
            (0, 16),
            (16, 24),
            (24, 32),
            (32, 40),
            (40, 43),
            (43, 48),
            (48, 50),
            (50, 52),
            (52, 57),
            (57, 62),
            (62, 64),
            (64, 73),
            (73, 82),
            (82, 91),
            (91, 100),
            (100, 109),
            (109, 118),
            (118, 127),
            (127, 136),
            (136, 145),
            (145, 154),
            (154, 162),
            (162, 170),
            (170, 178),
            (178, 186),
            (186, 194),
            (194, 202),
            (202, 203),
            (203, 206),
            (206, 208),
            (208, 212),
            (212, 216),
            (216, 218),
            (218, 220),
            (220, 221),
            (221, 231),
            (231, 232)
        ]

        col_names = [
            'MemberId',
            'ServiceDate',
            'AdmissionDate',
            'DischargeDate',
            'CoveredDays',
            'CPT',
            'CptMod1',
            'CptMod2',
            'HCPCS',
            'CPT2',
            'Cpt2Mod',
            'PrincipalIcdDiagnosis',
            'IcdDiagnosis2',
            'IcdDiagnosis3',
            'IcdDiagnosis4',
            'IcdDiagnosis5',
            'IcdDiagnosis6',
            'IcdDiagnosis7',
            'IcdDiagnosis8',
            'IcdDiagnosis9',
            'IcdDiagnosis10',
            'PrincipalIcdProcedure',
            'IcdProcedure2',
            'IcdProcedure3',
            'IcdProcedure4',
            'IcdProcedure5',
            'IcdProcedure6',
            'IcdIdentifier',
            'DRG',
            'DischargeStatus',
            'UbRevenue',
            'UbBillType',
            'NumberOfTimes',
            'CmsPlaceOfService',
            'ClaimStatus',
            'ProviderId',
            'SupplementalData'
        ]

        col_types = {
            'MemberId': np.unicode,
            'ServiceDate': np.object,
            'AdmissionDate': np.object,
            'DischargeDate': np.object,
            'CoveredDays': np.unicode,
            'CPT': np.unicode,
            'CptMod1': np.unicode,
            'CptMod2': np.unicode,
            'HCPCS': np.unicode,
            'CPT2': np.unicode,
            'Cpt2Mod': np.unicode,
            'PrincipalIcdDiagnosis': np.unicode,
            'IcdDiagnosis2': np.unicode,
            'IcdDiagnosis3': np.unicode,
            'IcdDiagnosis4': np.unicode,
            'IcdDiagnosis5': np.unicode,
            'IcdDiagnosis6': np.unicode,
            'IcdDiagnosis7': np.unicode,
            'IcdDiagnosis8': np.unicode,
            'IcdDiagnosis9': np.unicode,
            'IcdDiagnosis10': np.unicode,
            'PrincipalIcdProcedure': np.unicode,
            'IcdProcedure2': np.unicode,
            'IcdProcedure3': np.unicode,
            'IcdProcedure4': np.unicode,
            'IcdProcedure5': np.unicode,
            'IcdProcedure6': np.unicode,
            'IcdIdentifier': np.unicode,
            'DRG': np.unicode,
            'DischargeStatus': np.unicode,
            'UbRevenue': np.unicode,
            'UbBillType': np.unicode,
            'NumberOfTimes': np.unicode,
            'CmsPlaceOfService': np.unicode,
            'ClaimStatus': np.unicode,
            'ProviderId': np.unicode,
            'SupplementalData': np.unicode
        }

        self.log.info('Reading input file')
        df = pd.read_fwf(
            self.config.read_value('setup', 'visit.input.filename'),
            colspecs=col_breaks,
            names=col_names,
            dtype=col_types,
            parse_dates=[1, 2, 3]
        )

        for index, rows in df.iterrows():
            v = {
                'ServiceDate': self.process_date(rows.ServiceDate),
                'AdmissionDate': self.process_date(rows.AdmissionDate),
                'DischargeDate': self.process_date(rows.DischargeDate),
                'CoveredDays': self.validate_missing_field(rows.CoveredDays),
                'CPT': self.validate_missing_field(rows.CPT),
                'CptMod1': self.validate_missing_field(rows.CptMod1),
                'CptMod2': self.validate_missing_field(rows.CptMod2),
                'HCPCS': self.validate_missing_field(rows.HCPCS),
                'CPT2': self.validate_missing_field(rows.CPT2),
                'Cpt2Mod': self.validate_missing_field(rows.Cpt2Mod),
                'PrincipalIcdDiagnosis': self.validate_missing_field(rows.PrincipalIcdDiagnosis),
                'IcdDiagnosis2': self.validate_missing_field(rows.IcdDiagnosis2),
                'IcdDiagnosis3': self.validate_missing_field(rows.IcdDiagnosis3),
                'IcdDiagnosis4': self.validate_missing_field(rows.IcdDiagnosis4),
                'IcdDiagnosis5': self.validate_missing_field(rows.IcdDiagnosis5),
                'IcdDiagnosis6': self.validate_missing_field(rows.IcdDiagnosis6),
                'IcdDiagnosis7': self.validate_missing_field(rows.IcdDiagnosis7),
                'IcdDiagnosis8': self.validate_missing_field(rows.IcdDiagnosis8),
                'IcdDiagnosis9': self.validate_missing_field(rows.IcdDiagnosis9),
                'IcdDiagnosis10': self.validate_missing_field(rows.IcdDiagnosis10),
                'PrincipalIcdProcedure': self.validate_missing_field(rows.PrincipalIcdProcedure),
                'IcdProcedure2': self.validate_missing_field(rows.IcdProcedure2),
                'IcdProcedure3': self.validate_missing_field(rows.IcdProcedure3),
                'IcdProcedure4': self.validate_missing_field(rows.IcdProcedure4),
                'IcdProcedure5': self.validate_missing_field(rows.IcdProcedure5),
                'IcdProcedure6': self.validate_missing_field(rows.IcdProcedure6),
                'IcdIdentifier': self.validate_missing_field(rows.IcdIdentifier),
                'DRG': self.validate_missing_field(rows.DRG),
                'DischargeStatus': self.validate_missing_field(rows.DischargeStatus),
                'UbRevenue': self.validate_missing_field(rows.UbRevenue),
                'UbBillType': self.validate_missing_field(rows.UbBillType),
                'NumberOfTimes': self.validate_missing_field(rows.NumberOfTimes),
                'CmsPlaceOfService': self.validate_missing_field(rows.CmsPlaceOfService),
                'ClaimStatus': self.validate_missing_field(rows.ClaimStatus),
                'ProviderId': self.validate_missing_field(rows.ProviderId),
                'SupplementalData': self.validate_missing_field(rows.SupplementalData),
            }

            v['AggregatedCodes'] = self.aggregate_codes(v)

            member_id = rows.MemberId
            if member_id not in self.visits:
                self.visits[member_id] = [v]
            else:
                self.visits[member_id].append(v)

    @staticmethod
    def aggregate_codes(member_visit: dict) -> list:
        codes = []
        for code in [member_visit['CPT'],
                     member_visit['CptMod1'],
                     member_visit['CptMod2'],
                     member_visit['HCPCS'],
                     member_visit['CPT2'],
                     member_visit['Cpt2Mod'],
                     member_visit['PrincipalIcdDiagnosis'],
                     member_visit['IcdDiagnosis2'],
                     member_visit['IcdDiagnosis3'],
                     member_visit['IcdDiagnosis4'],
                     member_visit['IcdDiagnosis5'],
                     member_visit['IcdDiagnosis6'],
                     member_visit['IcdDiagnosis7'],
                     member_visit['IcdDiagnosis8'],
                     member_visit['IcdDiagnosis9'],
                     member_visit['IcdDiagnosis10'],
                     member_visit['PrincipalIcdProcedure'],
                     member_visit['IcdProcedure2'],
                     member_visit['IcdProcedure3'],
                     member_visit['IcdProcedure4'],
                     member_visit['IcdProcedure5'],
                     member_visit['IcdProcedure6'],
                     member_visit['IcdIdentifier'],
                     member_visit['DRG']]:
            if code:
                codes.append(code)

        return codes

    def write_visits(self):
        with open(self.config.read_value('setup', 'visit.output.filename'), 'w') as output_file:
            for (key, value) in self.visits.items():
                visit_with_key = {
                    'MemberId': key,
                    'visits': value
                }
                output_file.write(dumps(visit_with_key) + '\n')

    @staticmethod
    def process_date(visit_date: pd.Timestamp):
        if visit_date is pd.NaT:
            return None
        else:
            return visit_date.to_pydatetime().isoformat()

    @staticmethod
    def validate_missing_field(value):
        if value is np.NaN:
            return None
        else:
            return value


if __name__ == '__main__':
    visit = Visit()
    visit.process_visits()
    visit.write_visits()
