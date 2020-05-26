class ValueSet:
    from model.code import Code

    def __init__(
            self,
            vs_name: str,
            oid: str,
            codes: [Code]
    ):
        self.vs_name = vs_name
        self.oid = oid
        self.codes = codes

    def __str__(self):
        return 'Value Set Name {} OID {}'.format(self.vs_name, self.oid)
