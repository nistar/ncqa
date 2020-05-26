import pandas as pd
import logging
from util import natus_config, natus_logging
from db.mongo import connector


class PopulateValueset:
    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_con = connector.Connector()
        self.db_con.collection_name = 'oid_to_codes'
        self.excel_sheetname = self.config.read_value('valueset', 'excel.sheetname')
        self.excel_filename = self.config.read_value('valueset', 'excel.filename')

    def load_db(self):
        from collections import defaultdict

        self.log.info('Reading excel sheet ' + self.excel_sheetname)
        df = pd.read_excel(
            self.excel_filename,
            sheet_name=self.excel_sheetname
        )

        self.log.info('Processing excel data')
        oid_to_codes = defaultdict(list)
        for index, row in df.iterrows():
            oid = row['Value Set OID']
            code = row['Code']
            code_system = row['Code System']
            oid_to_codes[oid].append({
                'Code': code,
                'CodeSystem': code_system
                }
            )

        self.log.info('Loading value set')
        for oid, codes in oid_to_codes.items():
            self.db_con.insert({
                'OID': oid,
                'Codes': codes
            })


if __name__ == '__main__':
    pv = PopulateValueset()
    pv.load_db()
