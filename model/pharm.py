class Pharm:
    from datetime import datetime
    from db.mongo import connector

    def __init__(
            self,
            service_date: datetime,
            dispensed_med_code: str,
            db_con: connector.Connector
    ):

        self.service_date = service_date
        self.dispensed_med_code = dispensed_med_code[:9]
        self.dispensed_med_name = self.med_code_to_med_name(db_con)

    def __str__(self):
        return 'Service date: ' + self.service_date.strftime('%Y-%m-%d') + \
               ' Dispensed med: ' + str(self.dispensed_med_name) + \
               ' Dispensed med code ' + self.dispensed_med_code

    def med_code_to_med_name(self, db_con) -> [str]:
        med_name_ret = db_con.find('NDC', self.dispensed_med_code, 'ndc')
        return med_name_ret['DrugNames'] if med_name_ret is not None else ' '
