import logging
import numpy as np
import pandas as pd
from json import dumps
from util import natus_logging, natus_config, natus_util


class Provider:

    def __init__(self):
        self.config = natus_config.NATUSConfig('ncqa')
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.providers = {}

    def process_providers(self) -> None:
        col_breaks = [
            (0, 10),
            (10, 11),
            (11, 12),
            (12, 13),
            (13, 14),
            (14, 15),
            (15, 16),
            (16, 17),
            (17, 18),
            (18, 19),
            (19, 20),
            (20, 21),
            (21, 22),
            (22, 23),
            (23, 24),
            (24, 25)
        ]

        col_names = [
            'ProviderId',
            'PCP',
            'OBGYN',
            'MH',
            'EyeCare',
            'Dentist',
            'Nephrologist',
            'Anesthesiologist',
            'NursePractitioner',
            'PhysicianAssistant',
            'PrescribingPrivileges',
            'ClinicalPharmacist',
            'Hospital',
            'SkilledNursingFacility',
            'Surgeon',
            'RegisteredNurse'
        ]

        col_types = {
            'ProviderId': np.unicode,
            'PCP': np.unicode,
            'OBGYN': np.unicode,
            'MH': np.unicode,
            'EyeCare': np.unicode,
            'Dentist': np.unicode,
            'Nephrologist': np.unicode,
            'Anesthesiologist': np.unicode,
            'NursePractitioner': np.unicode,
            'PhysicianAssistant': np.unicode,
            'PrescribingPrivileges': np.unicode,
            'ClinicalPharmacist': np.unicode,
            'Hospital': np.unicode,
            'SkilledNursingFacility': np.unicode,
            'Surgeon': np.unicode,
            'RegisteredNurse': np.unicode
        }

        self.log.info('Reading input file')
        df = pd.read_fwf(
            self.config.read_value('setup', 'provider.input.filename'),
            colspecs=col_breaks,
            names=col_names,
            dtype=col_types
        )

        self.log.info('Writing json')
        with open(self.config.read_value('setup', 'provider.output.filename'), 'w') as output_file:
            for index, rows in df.iterrows():
                provider = {
                    'ProviderId': rows.ProviderId,
                    'PCP': natus_util.to_boolean(rows.PCP),
                    'OBGYN': natus_util.to_boolean(rows.OBGYN),
                    'MentalHealth': natus_util.to_boolean(rows.MH),
                    'EyeCare': natus_util.to_boolean(rows.EyeCare),
                    'Dentist': natus_util.to_boolean(rows.Dentist),
                    'Nephrologist': natus_util.to_boolean(rows.Nephrologist),
                    'Anesthesiologist': natus_util.to_boolean(rows.Anesthesiologist),
                    'NursePractitioner': natus_util.to_boolean(rows.NursePractitioner),
                    'PhysicianAssistant': natus_util.to_boolean(rows.PhysicianAssistant),
                    'PrescribingPrivileges': natus_util.to_boolean(rows.PrescribingPrivileges),
                    'ClinicalPharmacist': natus_util.to_boolean(rows.ClinicalPharmacist),
                    'Hospital': natus_util.to_boolean(rows.Hospital),
                    'SkilledNursingFacility': natus_util.to_boolean(rows.SkilledNursingFacility),
                    'Surgeon': natus_util.to_boolean(rows.Surgeon),
                    'RegisteredNurse': natus_util.to_boolean(rows.RegisteredNurse)
                }
                output_file.write(dumps(provider) + '\n')


if __name__ == '__main__':
    p = Provider()
    p.process_providers()
