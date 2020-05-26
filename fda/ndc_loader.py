import pandas as pd
import logging
from util import natus_config, natus_logging
from db.mongo import connector


class NDCLoader:
    def __init__(self):
        self.log = natus_logging.NATUSLogging(__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_con = connector.Connector()
        self.input_filename = self.config.read_value('ndc', 'input.csv.filename')

    def convert_to_11_digit(
            self,
            ndc_code: str
    ) -> str:
        # convert to 5-4-2 format. last 2 digits (package code) are not in the FDA file
        labeler, product_code = ndc_code.split(sep='-')
        if len(labeler) == 4 and len(product_code) == 4:
            return '0' + labeler + product_code
        elif len(labeler) == 5 and len(product_code) == 4:
            return labeler + product_code
        elif len(labeler) == 5 and len(product_code) == 3:
            return labeler + '0' + product_code
        else:
            self.log.error('Unknown code ' + ndc_code)

    def read_ndc(
            self
    ) -> dict:
        self.log.info('Reading NDC from ' + self.input_filename)
        df = pd.read_csv(
            self.input_filename,
            sep='\t',
            engine='python'
        )

        self.log.info('Processing NDC data')
        ndc_to_name = {}
        for index, row in df.iterrows():
            cms_representation = self.convert_to_11_digit(row['PRODUCTNDC'])
            proprietary_name = row['PROPRIETARYNAME']
            non_proprietary_name = row['NONPROPRIETARYNAME']
            ndc_to_name[cms_representation] = [proprietary_name, non_proprietary_name]

        return ndc_to_name

    def load_ndc(
            self,
            ndc_map: dict
    ):
        self.log.info('Loading NDC')
        for key, value in ndc_map.items():
            mapping = {
                'NDC': key,
                'DrugNames': value
            }
            self.db_con.insert(mapping, 'ndc')


if __name__ == '__main__':
    loader = NDCLoader()
    m = loader.read_ndc()
    loader.load_ndc(m)
