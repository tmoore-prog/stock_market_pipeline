from google.oauth2 import service_account
from dotenv import load_dotenv
import os
from pathlib import Path


try:
    from airflow.models import Variable
    IS_AIRFLOW = True

except ImportError:
    IS_AIRFLOW = False
    load_dotenv()


def get_config_value(key, default=None):
    if IS_AIRFLOW:
        return Variable.get(key, default_var=os.getenv(key, default))
    else:
        return os.getenv(key, default)


PROJECT_ROOT = Path(__file__).parent.parent

GCP_PROJECT_ID = get_config_value('GCP_PROJECT_ID')
BIGQUERY_DATASET = get_config_value('BIGQUERY_DATASET')
BIGQUERY_TABLE = get_config_value('BIGQUERY_TABLE')
CHECKPOINT_TABLE = get_config_value('CHECKPOINT_TABLE')

POLYGON_API_KEY = get_config_value('POLYGON_API_KEY')
API_BASE_URL = get_config_value('API_BASE_URL')

if IS_AIRFLOW:
    credentials_path = get_config_value('GOOGLE_APPLICATION_CREDENTIALS')
else:
    credentials_path = PROJECT_ROOT / 'keys' / 'gcpbigquery.json'

if os.path.exists(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(
        str(credentials_path),
        scopes=['https://www.googleapis.com/auth/bigquery']
        )
else:
    credentials = None
