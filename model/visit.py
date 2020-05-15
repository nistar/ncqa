
class Visit:
    from datetime import datetime

    def __init__(self, service_date: datetime, codes: [str]):
        self.service_date = service_date
        self.codes = codes

    def __str__(self):
        return 'Service Date: ' + self.service_date.strftime('%Y-%m-%d') + \
               ' Codes: ' + self.codes

