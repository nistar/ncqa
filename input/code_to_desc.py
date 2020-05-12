from util import natus_logging, natus_config
from db.mongo import connector
import logging


class Code2Desc:
    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_con = connector.Connector()
        self.db_con.collection_name = 'code_to_description'
        self.excel_sheetname = self.config.read_value('valueset', 'excel.sheetname')
        self.excel_filename = self.config.read_value('valueset', 'excel.filename')
        self.cpt_input_file = self.config.read_value('cms', 'cpt_input_file')

    def load_vs_descriptions(self):
        import pandas as pd

        self.log.info('Loading ICD descriptions')

        df = pd.read_excel(
            self.excel_filename,
            sheet_name=self.excel_sheetname
        )

        code_systems_with_definitions = [
            'ICD9PCS', 'ICD9CM', 'ICD10CM', 'SNOMED CT US Edition',
            'ICD10PCS', 'SOP', 'HCPCS', 'RXNORM', 'CPT-CAT-II', 'POS',
            'HL7', 'CVX', 'LOINC'
        ]

        for index, row in df.iterrows():
            code_system = row['Code System']
            if code_system in code_systems_with_definitions:
                code = row['Code']
                definition = row['Definition']
                self.db_con.insert({'Code': code, 'CodeSystem': code_system, 'Definition': definition},
                                   'code_to_description')

        self.log.info('Processing ' + self.cpt_input_file)

        processed_cpt_code = set()
        df = pd.read_csv(self.cpt_input_file)
        for index, row in df.iterrows():
            code = row['hcpc_code_id']
            if code not in processed_cpt_code:
                processed_cpt_code.add(code)
                self.db_con.insert(
                    {
                        'Code': str(code),
                        'CodeSystem': 'CPT',
                        'Definition': row['long_description']
                    },
                    'code_to_description'
                )


if __name__ == '__main__':
    c2d = Code2Desc()
    c2d.load_vs_descriptions()

