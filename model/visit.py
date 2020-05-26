
class Visit:
    from datetime import datetime

    def __init__(self, service_date: datetime, codes: [str], modifier: str):
        self.service_date = service_date
        self.codes = codes
        self.modifier = modifier

    def __str__(self):
        modifier = ' Modifier ' + self.modifier if self.modifier is not None else ''
        return 'Service Date: ' + self.service_date.strftime('%Y-%m-%d') + \
               ' Codes: ' + str(self.codes) + modifier

    def __lt__(self, other) -> bool:
        return self.service_date < other.service_date

