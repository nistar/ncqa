class Code:
    def __init__(
            self,
            code: str,
            code_system: str,
            oid: str
    ):
        self.code = code
        self.code_system = code_system
        self.oid = oid

    def __str__(self):
        return 'Code {} Code System {} OID {}'.format(
            self.code, self.code_system, self.oid)
