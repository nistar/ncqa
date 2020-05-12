import pandas as pd
from db.mongo import connector
import sys


def load_sample_file(
        params: dict
):
    db_connection = connector.Connector()
    db_connection.collection_name = params['TableName']
    df = pd.read_csv(params['Filename'])

    for _, row in df.iterrows():
        member_id = row['MemID']
        measure = row['Meas']
        payer = row['Payer']
        eligible_population = row['Epop']
        exclusion = row['Excl']
        numerator = row['Num']
        required_exclusion = row['RExcl']
        indicator = row['Ind']

        sample_data = {
            'MemberId': member_id,
            'Measure': measure,
            'Payer': payer,
            'EligiblePopulation': eligible_population,
            'Exclusion': exclusion,
            'Numerator': numerator,
            'RequiredExclusion': required_exclusion,
            'Indicator': indicator
        }

        db_connection.insert(sample_data)


def main():
    import argparse

    parser = argparse.ArgumentParser(
       description='Load sample result data into DB for easier verification'
    )

    parser.add_argument(
        '-f',
        '--filename',
        metavar='',
        type=str,
        help='Location of sample file'
    )

    parser.add_argument(
        '-n',
        '--hostname',
        metavar='',
        type=str,
        help='DB hostname'
    )

    parser.add_argument(
        '-d',
        '--dbname',
        metavar='',
        type=str,
        help='DB name.'
    )

    parser.add_argument(
        '-t',
        '--tablename',
        metavar='',
        type=str,
        help='Table name.'
    )

    parser.add_argument(
        '-p',
        '--port',
        metavar='',
        type=int,
        help='DB port'
    )

    parser.add_argument(
        '-v',
        '--vendor',
        metavar='',
        type=str,
        help='DB vendor. Valid entries are: mongodb, oracle, postgres'
    )

    parser.add_argument(
        '-URI',
        '--URIAddress',
        metavar='',
        type=str,
        help='Full DB URI. For example: mongodb://localhost:27017'
    )

    args = parser.parse_args()

    params = {
        'TableName': args.tablename,
        'DatabaseName': args.dbname,
        'URI':  args.URIAddress,
        'Filename': args.filename
    }
    load_sample_file(params)


if __name__ == '__main__':
    sys.exit(main())
