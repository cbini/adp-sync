#!/python3.8
import gzip
import json
import os
import pathlib

import requests
from dotenv import load_dotenv
from google.cloud import storage
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

load_dotenv()

CERT_FILENAME = os.getenv("CERT_FILENAME")
KEY_FILENAME = os.getenv("KEY_FILENAME")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

SECURITY_TOKEN_SERVICE = "accounts.adp.com"
SERVICE_ROOT = "api.adp.com"
PROJECT_PATH = pathlib.Path(__file__).absolute().parent

CERT_PATH = PROJECT_PATH / "certs" / CERT_FILENAME
KEY_PATH = PROJECT_PATH / "certs" / KEY_FILENAME
CERT_TUPLE = (CERT_PATH, KEY_PATH)

## instantiate ADP client
token_url = f"https://{SECURITY_TOKEN_SERVICE}/auth/oauth/v2/token"
auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
client = BackendApplicationClient(client_id=CLIENT_ID)
session = OAuth2Session(client=client)
session.cert = CERT_TUPLE

## authorize ADP client
token_dict = session.fetch_token(token_url=token_url, auth=auth)
access_token = token_dict.get('access_token')
session.headers["Authorization"] = f"Bearer {access_token}"

## instantiate GCS clinet
gcs_storage_client = storage.Client()
gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

## define endpoint variables
endpoint = "/hr/v2/workers"
table_name = endpoint.replace("/", "_")
print(f"{endpoint}")

data_path = PROJECT_PATH / "data" / table_name
data_file = data_path / f"{table_name}.json.gz"
if not data_path.exists():
    data_path.mkdir(parents=True)
    print(f"\tCreated {'/'.join(data_path.parts[-3:])}...")

url = f"https://{SERVICE_ROOT}{endpoint}"
querystring = {
    "$select": ','.join([
        "worker/associateOID",
        "worker/person/preferredName",
        "worker/person/legalName",
        "worker/businessCommunication/emails",
        "worker/customFieldGroup",
        "worker/workerDates",
    ]),
    "$skip": 0,
}

## pull all data from endpoint # TODO: refactor into function
all_data = []
while True:
    r = session.get(url, params=querystring)
    if r.status_code == 204:
        break
    else:
        data = r.json()
        all_data.extend(data.get("workers"))
        querystring["$skip"] += 50

## save to json.gz
with gzip.open(data_file, "wt", encoding="utf-8") as f:
    json.dump(all_data, f)
print(f"\tSaved to {'/'.join(data_file.parts[-4:])}!")

## upload to GCS
destination_blob_name = "adp/" + "/".join(data_file.parts[-2:])
blob = gcs_bucket.blob(destination_blob_name)
blob.upload_from_filename(data_file)
print(f"\tUploaded to {destination_blob_name}!")
