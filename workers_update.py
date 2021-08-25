import gzip
import json
import os
import pathlib
import traceback

import requests
from dotenv import load_dotenv
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from datarobot.utilities import email

load_dotenv()

CERT_FILENAME = os.getenv("CERT_FILENAME")
KEY_FILENAME = os.getenv("KEY_FILENAME")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
ADP_IMPORT_FILE = os.getenv("ADP_IMPORT_FILE")
ADP_EXPORT_FILE = os.getenv("ADP_EXPORT_FILE")

SECURITY_TOKEN_SERVICE = "accounts.adp.com"
SERVICE_ROOT = "api.adp.com"
WORKER_ENDPOINT = "/events/hr/v1/worker"
TOKEN_URL = f"https://{SECURITY_TOKEN_SERVICE}/auth/oauth/v2/token"

PROJECT_PATH = pathlib.Path(__file__).absolute().parent
CERT_PATH = PROJECT_PATH / "certs" / CERT_FILENAME
KEY_PATH = PROJECT_PATH / "certs" / KEY_FILENAME
CERT_TUPLE = (CERT_PATH, KEY_PATH)


def post_worker_change(session, path, payload):
    change_url = f"https://{SERVICE_ROOT}{WORKER_ENDPOINT}.{path}.change"

    try:
        r_change = session.post(change_url, json=payload)
        r_change.raise_for_status()
    except:
        resource_messages = (
            r_change.json().get("confirmMessage").get("resourceMessages")
        )
        process_messages = [m.get("processMessages") for m in resource_messages]

        formatted_message = f"\t{change_url}\n\t{payload}\n\n"
        for m in process_messages:
            print(f"message: {m}")
            formatted_message += (
                f"\t{r_change.status_code} - {r_change.reason}: "
                f"{m.get('userMessage').get('messageTxt')}"
            )
        print(formatted_message)

        email_body = f"{formatted_message}\n\n{traceback.format_exc()}"
        email.send_email("ADP Worker Update Error", email_body)


def main():
    print("Authenticating with ADP...")
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    client = BackendApplicationClient(client_id=CLIENT_ID)
    session = OAuth2Session(client=client)
    session.cert = CERT_TUPLE
    token_dict = session.fetch_token(token_url=TOKEN_URL, auth=auth)

    # attach auth to session
    access_token = token_dict.get("access_token")
    session.headers["Authorization"] = f"Bearer {access_token}"
    print("\tSUCCESS!")

    print("Loading db import data...")
    with open(ADP_IMPORT_FILE, "r") as f:
        import_data = json.load(f)
    print("\tSUCCESS!")

    print("Loading ADP export data...")
    with gzip.open(ADP_EXPORT_FILE, "r") as f:
        workers_export_data = json.loads(f.read().decode("utf-8"))
    print("\tSUCCESS!")

    print("Flattening ADP export data...")
    workers_export_clean = []
    for w in workers_export_data:
        w_clean = {}
        w_clean["associateOID"] = w["associateOID"]
        w_clean["work_email"] = next(
            iter(
                [
                    e.get("emailUri")
                    for e in w["businessCommunication"].get("emails", {})
                    if e["nameCode"]["codeValue"] == "Work E-mail"
                ]
            ),
            None,
        )
        employee_number = next(
            iter(
                [
                    f
                    for f in w["customFieldGroup"].get("stringFields", {})
                    if f["nameCode"]["codeValue"] == "Employee Number"
                ]
            ),
            None,
        )
        w_clean["employee_number"] = employee_number
        workers_export_clean.append(w_clean)
    print("\tSUCCESS!")

    print("Processing ADP updates...")
    for i in import_data:
        base_payload = {
            "events": [
                {
                    "data": {
                        "eventContext": {
                            "worker": {"associateOID": i["associate_oid"]}
                        },
                        "transform": {"worker": {}},
                    }
                }
            ]
        }

        # match db record to ADP record
        record_match = next(
            iter(
                [
                    w
                    for w in workers_export_clean
                    if w["associateOID"] == i["associate_oid"]
                ]
            ),
            None,
        )

        if record_match:
            ## update work email if new
            if i["mail"] != record_match["work_email"]:
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match['work_email']} => {i['mail']}"
                )

                work_email_change_payload = base_payload.copy()
                work_email_change_payload["events"][0]["data"]["transform"][
                    "worker"
                ] = {"businessCommunication": {"email": {"emailUri": i["mail"]}}}

                post_worker_change(
                    session=session,
                    path="business-communication.email",
                    payload=work_email_change_payload,
                )

            # update employee number if missing
            if not record_match.get("employee_number").get("stringValue"):
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('employee_number').get('stringValue')}"
                    f" => {i['employee_number']}"
                )

                employee_number_change_payload = base_payload.copy()
                employee_number_change_payload["events"][0]["data"]["eventContext"][
                    "worker"
                ]["customFieldGroup"] = {
                    "stringField": {
                        "itemID": f"{record_match.get('employee_number').get('itemID')}"
                    }
                }
                employee_number_change_payload["events"][0]["data"]["transform"][
                    "worker"
                ]["customFieldGroup"] = {
                    "stringField": {"stringValue": i["employee_number"]}
                }

                post_worker_change(
                    session=session,
                    path="custom-field.string",
                    payload=employee_number_change_payload,
                )

    print("SUCCESS!")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        email_body = f"{traceback.format_exc()}"
        email.send_email("ADP Worker Update Error", email_body)
