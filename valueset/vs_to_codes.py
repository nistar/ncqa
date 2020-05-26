
def vs_to_codes():
    from util import natus_config
    import pandas as pd
    from model.code import Code
    from model.valueset import ValueSet
    from db.mongo import connector
    import json

    config = natus_config.NATUSConfig('ncqa')
    excel_filename = config.read_value('valueset', 'excel_filename')
    sheet_name = config.read_value('valueset', 'excel_sheet_name')

    print('Reading ' + excel_filename)
    df = pd.read_excel(
        excel_filename,
        sheet_name=sheet_name
    )

    print('Processing ' + sheet_name)
    value_set_info = None
    codes_processed = set()
    db_con = connector.Connector()

    for index, row in df.iterrows():
        vs_name = row['Value Set Name']
        code = row['Code']
        code_system = row['Code System']
        code_system_oid = row['Code System OID']
        code_info = Code(code, code_system, code_system_oid)
        if vs_name not in codes_processed:
            if len(codes_processed) > 0:
                db_con.insert(vars(value_set_info), 'vs_to_codes')
            codes_processed.add(vs_name)
            vs_oid = row['Value Set OID']
            value_set_info = ValueSet(vs_name, vs_oid, [vars(code_info)])
        else:
            value_set_info.codes.append(vars(code_info))


if __name__ == '__main__':
    vs_to_codes()
