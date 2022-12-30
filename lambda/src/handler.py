import json
import logging
import datetime
from etl.extract import extract
from etl.transform import transform
from etl.load import load

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

def main(event, context):
    LOG.info(f"Event: {json.dumps(event)}")
    try:
        users = extract(limit=100)
        countries = transform(users)
        load(countries)
    except:
        return {
        'statusCode': 200,
        'body': {
            "message": f"ETL job failed {datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')}"
        }
    }

    return {
        'statusCode': 200,
        'body': {
            "message": f"ETL job completed {datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')}"
        }
    }