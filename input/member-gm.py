import pandas as pd
import numpy as np
import logging
from json import dumps
from util import natus_config, natus_logging


class MemberGM:

    def __init__(self):
        self.config = natus_config.NATUSConfig('ncqa')
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)

    def race_description(self, race_code: str):
        return self.config['race'][race_code]

    def ethnicity_description(self, eth_code: str):
        return self.config['ethnicity'][eth_code]

    def read_member_gm(self):
        col_breaks = [(0, 16),
                      (16, 17),
                      (17, 25),
                      (25, 45),
                      (45, 65),
                      (65, 66),
                      (66, 82),
                      (82, 132),
                      (132, 182),
                      (182, 212),
                      (212, 214),
                      (214, 219),
                      (219, 229),
                      (229, 254),
                      (254, 255),
                      (255, 280),
                      (280, 282),
                      (282, 284),
                      (284, 286),
                      (286, 288),
                      (288, 290),
                      (290, 292),
                      (292, 294),
                      (294, 296),
                      (296, 298),
                      (298, 300)]
        col_names = ['MemberId',
                     'Gender',
                     'DOB',
                     'LastName',
                     'FirstName',
                     'MI',
                     'SubscriberId',
                     'MailAddress1',
                     'MailAddress2',
                     'City',
                     'State',
                     'Zip',
                     'Phone',
                     'CaretakerFirstName',
                     'CaretakerMI',
                     'CaretakerLastName',
                     'Race',
                     'Ethnicity',
                     'RaceDataSource',
                     'EthnicityDataSource',
                     'SpokenLanguage',
                     'SpokenLanguageSource',
                     'WrittenLanguage',
                     'WrittenLanguageSource',
                     'OtherLanguage',
                     'OtherLanguageSource']
        col_types = {'MemberId': np.unicode,
                     'Gender': np.unicode,
                     'DOB': np.object,
                     'LastName': np.unicode,
                     'FirstName': np.unicode,
                     'MI': np.unicode,
                     'SubscriberId': np.unicode,
                     'MailAddress1': np.unicode,
                     'MailAddress2': np.unicode,
                     'City': np.unicode,
                     'State': np.unicode,
                     'Zip': np.unicode,
                     'Phone': np.unicode,
                     'CaretakerFirstName': np.unicode,
                     'CaretakerMI': np.unicode,
                     'CaretakerLastName': np.unicode,
                     'Race': np.unicode,
                     'Ethnicity': np.unicode,
                     'RaceDataSource': np.unicode,
                     'EthnicityDataSource': np.unicode,
                     'SpokenLanguage': np.unicode,
                     'SpokenLanguageSource': np.unicode,
                     'WrittenLanguage': np.unicode,
                     'WrittenLanguageSource': np.unicode,
                     'OtherLanguage': np.unicode,
                     'OtherLanguageSource': np.unicode}

        df = pd.read_fwf(
            self.config.read_value('setup', 'member-gm.input.file.name'),
            colspecs=col_breaks,
            names=col_names,
            dtype=col_types,
            parse_dates=[2]
        )

        with open(self.config.read_value('setup', 'member-gm.output.file.name'), 'w') as out_file:

            for index, rows in df.iterrows():
                member = {
                    'MemberId': rows.MemberId,
                    'Gender': rows.Gender,
                    'DOB': rows.DOB.to_pydatetime().isoformat(),
                    'LastName': rows.LastName,
                    'FirstName': rows.FirstName,
                    'MI': rows.MI,
                    'SubscriberId': rows.SubscriberId,
                    'MailAddress1': rows.MailAddress1,
                    'MailAddress2': rows.MailAddress2,
                    'City': rows.City,
                    'State': rows.State,
                    'Zip': rows.Zip,
                    'Phone': rows.Phone,
                    'CaretakerFirstName': rows.CaretakerFirstName,
                    'Race': rows.Race,
                    'Ethnicity': rows.Ethnicity,
                    'RaceDataSource': rows.RaceDataSource,
                    'EthnicityDataSource': rows.EthnicityDataSource,
                    'SpokenLanguage': rows.SpokenLanguage,
                    'SpokenLanguageSource': rows.SpokenLanguageSource,
                    'WrittenLanguage': rows.WrittenLanguage,
                    'WrittenLanguageSource': rows.WrittenLanguageSource,
                    'OtherLanguage': rows.OtherLanguage,
                    'OtherLanguageSource': rows.OtherLanguageSource}
                member_json = dumps(member)
                out_file.write(member_json + '\n')


if __name__ == '__main__':

    member_gm = MemberGM()
    member_gm.read_member_gm()

