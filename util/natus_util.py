from datetime import datetime
from jsonpath_ng import parse
from db.mongo import connector


def measure_to_codes(measure: str, connection: connector.Connector) -> dict:
    measure_oids = connection.find('MeasureID', measure, 'measure_to_oid')
    cursor = connection.find_all('OID', {'$in': measure_oids['OIDs']}, 'oid_to_codes')
    codes = {}

    for oid_2_codes in cursor:
        for code_info in oid_2_codes['Codes']:
            code = code_info['Code']
            code_2_description = connection.find('Code', code, 'code_to_description')

            codes[code] = {
                'CodeSystem': code_info['CodeSystem'],
                'CodeDescription': None if code_2_description is None else code_2_description['Definition']
            }

    return codes


def to_boolean(input_char: str) -> bool:
    if input_char == 'Y':
        return True
    else:
        return False


def measurement_year_range(run_date: datetime, measurements: []) -> range:
    return range(run_date.year - (len(measurements) - 1), run_date.year + 1)


def measurement_year_index(year_range: range, year: int) -> int:
    return year_range.index(year)


def json_path_query(query: str, data: dict):
    path_expr = parse(query)
    return path_expr.find(data)


def continuous_enrollment(
        continuous_enrollment_config: list
) -> list:
    from pandas import date_range

    continuous_enrollment_date_ranges = []
    for ce in continuous_enrollment_config:
        continuous_enrollment_date_ranges.append(
            {
                "DateRange": date_range(ce['From'], ce['To']),
                "AllowableGap": ce['AllowableGap']
            }
        )
    return continuous_enrollment_date_ranges


def age(dob: str, run_date: datetime) -> int:
    dob_dt = datetime.fromisoformat(dob)
    return run_date.year - dob_dt.year - ((run_date.month, run_date.day) < (dob_dt.month, dob_dt.day))

