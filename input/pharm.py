import logging
import numpy as np
import pandas as pd
from json import dumps
from util import natus_logging, natus_config


class Pharm:

    def __init__(self):
        self.config = natus_config.NATUSConfig('ncqa')
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.pharm = {}

    def process_pharm(self):
        col_breaks = [
            (0, 16),
            (16, 19),
            (19, 27),
            (27, 38),
            (38, 39),
            (39, 43),
            (43, 50),
            (50, 51),
            (51, 61),
            (61, 71)
        ]

        col_names = [
            'MemberId',
            'DaysSupply',
            'ServiceDate',
            'NDCDrugCode',
            'ClaimStatus',
            'MetricQuantity',
            'QuantityDispensed',
            'SupplementalData',
            'ProviderNPI',
            'PharmacyNPI'
        ]

        col_types = {
            'MemberId': np.unicode,
            'DaysSupply': np.unicode,
            'ServiceDate': np.object,
            'NDCDrugCode': np.unicode,
            'ClaimStatus': np.unicode,
            'MetricQuantity': np.unicode,
            'QuantityDispensed': np.unicode,
            'SupplementalData': np.unicode,
            'ProviderNPI': np.unicode,
            'PharmacyNPI': np.unicode
        }

        self.log.info('Reading input file')
        df = pd.read_fwf(
            self.config.read_value('setup', 'pharm.input.filename'),
            colspecs=col_breaks,
            names=col_names,
            dtype=col_types,
            parse_dates=[2]
        )

        self.log.info('Writing json')
        with open(self.config.read_value('setup', 'pharm.output.filename'), 'w') as output_file:
            # claim status '1' - paid; '2' - denied
            for index, rows in df.iterrows():
                pharm = {
                    'MemberId': rows.MemberId,
                    'DaysSupply': int(rows.DaysSupply),
                    'ServiceDate': rows.ServiceDate.to_pydatetime().isoformat(),
                    'NDCDrugCode': rows.NDCDrugCode,
                    'ClaimStatus': rows.ClaimStatus,
                    'MetricQuantity': int(rows.MetricQuantity),
                    'QuantityDispensed': int(rows.QuantityDispensed),
                    'SupplementalData': rows.SupplementalData,
                    'ProviderNPI': self.sanitize_str(rows.ProviderNPI),
                    'PharmacyNPI': self.sanitize_str(rows.PharmacyNPI)
                }
                output_file.write(dumps(pharm) + '\n')

    @staticmethod
    def sanitize_str(value) -> str:
        if value is np.NaN:
            return ''
        return value


if __name__ == '__main__':
    p = Pharm()
    p.process_pharm()
