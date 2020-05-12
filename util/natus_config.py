import configparser
from os import environ
import logging


class NATUSConfig:
    def __init__(self, client_name: str):
        self.client_name = client_name
        self.config = configparser.ConfigParser()
        self.config.read(self.__get_client_config_loc__())
        self.log = logging.getLogger(self.__class__.__name__)

    def __get_client_config_loc__(self):
        return environ['HOME'] + '/clients/' + self.client_name + '/config.ini'

    def read_value(self, section: str, key: str):
        return self.config[section][key]

