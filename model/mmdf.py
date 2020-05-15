class MMDF:
    from datetime import datetime

    def __init__(self, run_date: datetime, lti_flag: bool):
        self.run_date = run_date
        self.lti_flag = lti_flag

    def __str__(self):
        return 'Run Date: ' + self.run_date.strftime('%Y-%m-%d') + \
               ' LTI Flag: ' + str(self.lti_flag)
