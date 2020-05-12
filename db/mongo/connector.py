import pymongo
import logging
from util import natus_logging, natus_config


class Connector:

    def __init__(self):
        self.log = natus_logging.NATUSLogging(self.__class__.__name__, logging.INFO)
        self.config = natus_config.NATUSConfig('ncqa')
        self.dbname = self.config.read_value('db', 'mongodb.db.name')
        self.url = self.config.read_value('db', 'mongodb.url')
        self.log.info('Connecting to mongodb. db: ' + self.dbname + ', url: ' + self.url)
        self.client = pymongo.MongoClient(self.url)
        self.db = self.client[self.dbname]

    def find(self, field_name: str, field_value, collection_name: str):
        collection = self.db[collection_name]
        return collection.find_one({field_name: field_value})

    def find_all(self, field_name: str, field_value, collection_name: str) -> pymongo.cursor.Cursor:
        collection = self.db[collection_name]
        return collection.find({field_name: field_value})

    def read(self, collection_name: str):
        collection = self.db[collection_name]
        return collection.find()

    def insert(self, value, collection_name: str):
        collection = self.db[collection_name]
        return collection.insert_one(value)

    def count(self, collection_name: str):
        collection = self.db[collection_name]
        return collection.count_documents()

    def bulk_insert(self, member: list, collection_name: str):
        self.log.info('Inserting ' + str(len(member)) + ' docs')
        collection = self.db[collection_name]
        return collection.insert_many(member)


if __name__ == '__main__':
    from datetime import datetime
    conn = Connector()
    conn.collection_name = 'log'
    conn.insert({"alex": datetime.today()})
