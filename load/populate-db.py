from db.mongo import connector
import logging
from util import natus_config, natus_logging
from json import loads


class PopulateDb:

    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.db_connection = connector.Connector()
        self.bulk_size = self.config.read_value('load', 'bulk.size')
        self.input_file_name = self.config.read_value('load', 'input.filename')
        self.documents = []

    def bulk_insert(self):
        inserted_docs = 0
        # send a bulk size batch of docs
        docs_to_insert = []
        for doc in self.documents:
            if inserted_docs == int(self.bulk_size):
                self.db_connection.bulk_insert(docs_to_insert, 'member')
                docs_to_insert.clear()
                inserted_docs = 0
            else:
                docs_to_insert.append(doc)
                inserted_docs += 1

        # insert last batch
        self.db_connection.bulk_insert(docs_to_insert, 'member')

    def read_merged_data(self):
        with open(self.input_file_name) as input_file:
            for line in input_file:
                member_json = loads(line)
                self.documents.append(member_json)


if __name__ == '__main__':
    p_db = PopulateDb()
    p_db.read_merged_data()
    p_db.bulk_insert()
