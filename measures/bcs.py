import json
from datetime import datetime
from dotmap import DotMap
from util import natus_config
from db.mongo import connector


class BCS:

    def __init__(
            self,
            run_date: datetime,
            connection: connector.Connector
    ):
        from util import natus_util
        config = natus_config.NATUSConfig('ncqa')
        # shorthand notation
        config_section_name = self.__class__.__name__
        self.codes = natus_util.measure_to_codes(config_section_name, connection)
        self.anchor_date = eval(config.read_value(config_section_name, 'anchor.date'))
        continuous_enrollment_config = eval(config.read_value(config_section_name, "continuous.enrollment"))
        self.continuous_enrollment = natus_util.continuous_enrollment(continuous_enrollment_config)

    def __str__(self):
        display = 'Anchor Date: ' + self.anchor_date.strftime('%Y-%m-%d') + ' Continuous Enrollment:'
        for enrollment_year in self.continuous_enrollment:
            display += ' [From: ' + enrollment_year['DateRange'][0].strftime('%Y-%m-%d') + \
                ' To: ' + enrollment_year['DateRange'][-1].strftime('%Y-%m-%d') + \
                '] Allowable Gap: ' + str(enrollment_year['AllowableGap']) + '; '
        return display

    @staticmethod
    def run(member: dict, context: DotMap):
        pass


if __name__ == '__main__':
    num = BCS(datetime(2018, 12, 31))
