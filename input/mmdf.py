import glob
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from json import dumps

from util import natus_logging, natus_config


class Mmdf:

    def __init__(self):
        self.config = natus_config.NATUSConfig('ncqa')
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.mmdf_file_glob = glob.glob(self.config.read_value('setup', 'mmdf.input.filename.regex'))
        self.mmdf = {}

    def process_mmdf(self):

        col_breaks = [
            (5, 13),
            (13, 19),
            (19, 31),
            (60, 61),
            (66, 67)
        ]

        # LTI: Long term institutional status
        col_names = [
            'Rundate',
            'PaymentDate',
            'MemberId',
            'Hospice',
            'LTI'
        ]

        col_types = {
            'Rundate': np.object,
            'PaymentDate': np.unicode,
            'MemberId': np.unicode,
            'Hospice': np.unicode,
            'LTI': np.unicode
        }

        for mmdf_file in self.mmdf_file_glob:
            df = pd.read_fwf(
                mmdf_file,
                colspecs=col_breaks,
                names=col_names,
                dtype=col_types,
                parse_dates=[0]
            )

            for index, rows in df.iterrows():
                payment_date = datetime.strptime(rows.PaymentDate, '%Y%m').isoformat()
                run_date = rows.Rundate.to_pydatetime().isoformat()
                member_id = rows.MemberId
                member_mmdf = {
                    'Rundate': run_date,
                    'PaymentDate': payment_date,
                    'Hospice': self.to_boolean(rows.Hospice),
                    'LongTermInstitutionalStatus': self.to_boolean(rows.LTI)
                }

                if member_id in self.mmdf:
                    self.mmdf[member_id].append(member_mmdf)
                else:
                    self.mmdf[member_id] = [member_mmdf]

    def write_mmdf(self):
        with open(self.config.read_value('setup', 'mmdf.output.filename'), 'w') as output_file:
            for key, value in self.mmdf.items():
                mmdf_json = {
                    'MemberId': key,
                    'HospiceLti': value
                }
                output_file.write(dumps(mmdf_json) + '\n')

    @staticmethod
    def to_boolean(value) -> bool:
        if value is np.NaN:
            return False
        return True


if __name__ == '__main__':
    mmdf = Mmdf()
    mmdf.process_mmdf()
    mmdf.write_mmdf()
