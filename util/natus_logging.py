import logging
from util import natus_config

DESTINATION_FLUENTD_MONGODB = 0
DESTINATION_PY_LOGGING = 1


class NATUSLogging:

    def __init__(
            self,
            logger_name: str,
            level: int,
            destination: int = DESTINATION_PY_LOGGING
    ):
        from fluent import handler

        self.config = natus_config.NATUSConfig('ncqa')
        self.destination = destination
        self.log = logging.getLogger(logger_name)
        self.log.setLevel(level)

        if self.destination == DESTINATION_PY_LOGGING:
            ch = logging.StreamHandler()
            formatter = logging.Formatter(self.config.read_value('log', 'py.log.format'))
            ch.setFormatter(formatter)
            self.log.addHandler(ch)
        elif self.destination == DESTINATION_FLUENTD_MONGODB:
            self.handler = handler.FluentHandler(
                logger_name,
                host=self.config.read_value('log', 'fluentd.hostname'),
                port=int(self.config.read_value('log', 'fluentd.forward.port')),
            )
            custom_format = {
                'host': '%(hostname)s',
                'where': '%(module)s.%(funcName)s',
                'type': '%(levelname)s',
                'stack_trace': '%(exc_text)s'
            }
            formatter = handler.FluentRecordFormatter(custom_format)
            self.handler.setFormatter(formatter)
            self.log.addHandler(self.handler)
        else:
            print('Unrecognized log destination')
            raise ValueError

    def info(self, message: str) -> None:
        self.log.info(message)

    def debug(self, message: str) -> None:
        self.log.debug(message)

    def error(self, message: str) -> None:
        self.log.error(message)


if __name__ == '__main__':
    log = NATUSLogging('mongo', logging.INFO, DESTINATION_FLUENTD_MONGODB)
    log.info('Alex')
    log.handler.close()
