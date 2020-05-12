from util import natus_config, natus_logging
from db.mongo import connector
import logging
import pandas as pd


class MeasuresToValueSets:
    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_con = connector.Connector()
        self.db_con.collection_name = 'measure_to_oid'
        self.excel_sheetname = self.config.read_value('valueset', 'excel.measures.valuesets.sheet.name')
        self.excel_filename = self.config.read_value('valueset', 'excel.filename')

    def load_db(self):

        from collections import defaultdict

        self.log.info('Reading excel sheet ' + self.excel_sheetname)
        df = pd.read_excel(
            self.excel_filename,
            sheet_name=self.excel_sheetname
        )

        self.log.info('Processing excel data')
        measure_to_oids = defaultdict(list)

        for index, row in df.iterrows():
            measure_id = row['Measure ID']
            oid = row['Value Set OID']
            measure_to_oids[measure_id].append(oid)

        self.log.info('Loading measure to OIDs')

        for measure, oids in measure_to_oids.items():
            m_2_oids = {
                'MeasureID': measure,
                'OIDs': oids
            }
            self.db_con.insert(m_2_oids)


if __name__ == '__main__':
    mvs = MeasuresToValueSets()
    mvs.load_db()
