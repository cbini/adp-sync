import copy
import gzip
import json
import os
import pathlib
import traceback

from dotenv import load_dotenv

import adp

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CERT_FILEPATH = os.getenv("CERT_FILEPATH")
KEY_FILEPATH = os.getenv("KEY_FILEPATH")
ADP_IMPORT_FILE = os.getenv("ADP_IMPORT_FILE")
ADP_EXPORT_FILE = os.getenv("ADP_EXPORT_FILE")

WORKER_ENDPOINT = "/events/hr/v1/worker"

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def main():
    print("Authenticating with ADP...")
    adp_client = adp.authorize(CLIENT_ID, CLIENT_SECRET, CERT_FILEPATH, KEY_FILEPATH)
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
        w_clean["employee_number"] = next(
            iter(
                [
                    f
                    for f in w["customFieldGroup"].get("stringFields", {})
                    if f["nameCode"]["codeValue"] == "Employee Number"
                ]
            ),
            None,
        )
        w_clean["wfm_trigger"] = next(
            iter(
                [
                    f
                    for f in w["customFieldGroup"].get("stringFields", {})
                    if f["nameCode"]["codeValue"] == "WFMgr Trigger"
                ]
            ),
            None,
        )
        w_clean["pref_race_eth"] = next(
            iter(
                [
                    f
                    for f in w["person"]["customFieldGroup"].get("multiCodeFields", {})
                    if f["nameCode"]["codeValue"] == "Preferred Race/Ethnicity"
                ]
            ),
            None,
        )
        workers_export_clean.append(w_clean)
    print("\tSUCCESS!")

    print("Processing ADP updates...")
    for i in import_data:
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
            base_event_data = {
                "data": {
                    "eventContext": {"worker": {"associateOID": i["associate_oid"]}},
                    "transform": {"worker": {}},
                }
            }

            # update work email if new
            if i["mail"] != record_match["work_email"]:
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match['work_email']} => {i['mail']}"
                )

                work_email = copy.deepcopy(base_event_data)
                work_email_data = work_email["data"]["transform"]["worker"] = {
                    "businessCommunication": {"email": {"emailUri": i["mail"]}}
                }
                work_email_payload = {"events": [work_email_data]}

                adp.post(
                    session=adp_client,
                    endpoint=WORKER_ENDPOINT,
                    subresource="business-communication.email",
                    verb="change",
                    payload=work_email_payload,
                )

            # update employee number if missing
            if not record_match.get("employee_number").get("stringValue"):
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('employee_number').get('stringValue')}"
                    f" => {i['employee_number']}"
                )

                emp_num = copy.deepcopy(base_event_data)
                emp_num_data = emp_num["data"]["eventContext"][
                    "worker"
                ]["customFieldGroup"] = {
                    "stringField": {
                        "itemID": f"{record_match.get('employee_number').get('itemID')}"
                    }
                }
                emp_num_data = emp_num["data"]["transform"]["worker"][
                    "customFieldGroup"
                ] = {"stringField": {"stringValue": i["employee_number"]}}
                emp_num_payload = {"events": [emp_num_data]}

                adp.post(
                    session=adp_client,
                    endpoint=WORKER_ENDPOINT,
                    subresource="custom-field.string",
                    verb="change",
                    payload=emp_num_payload,
                )

            # update wfn trigger if not null
            if i["wfm_trigger"]:
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('wfm_trigger').get('stringValue')}"
                    f" => {i['wfm_trigger']}"
                )

                wfm_payload = copy.deepcopy(base_payload)
                wfm_payload["events"][0]["data"]["eventContext"]["worker"][
                    "customFieldGroup"
                ] = {
                    "stringField": {
                        "itemID": f"{record_match.get('wfm_trigger').get('itemID')}"
                    }
                }
                wfm_payload["events"][0]["data"]["transform"]["worker"][
                    "customFieldGroup"
                ] = {"stringField": {"stringValue": i["wfm_trigger"]}}

                adp.post(
                    session=adp_client,
                    endpoint=WORKER_ENDPOINT,
                    subresource="custom-field.string",
                    verb="change",
                    payload=wfm_payload,
                )

            # update pref race/eth if not matching
            i["pref_race_eth"].sort()
            rm_race_values = [
                c.get("codeValue")
                for c in record_match.get("pref_race_eth").get("codes")
            ]
            rm_race_values.sort()
            if i["pref_race_eth"] != rm_race_values:
                print(
                    f"{i['employee_number']}"
                    f"\t{rm_race_values}"
                    f" => {i['pref_race_eth']}"
                )

                race_payload = copy.deepcopy(base_payload)
                race_payload["events"][0]["data"]["eventContext"]["worker"][
                    "person"
                ] = {
                    "customFieldGroup": {
                        "codeField": {
                            "itemID": f"{record_match.get('pref_race_eth').get('itemID')}"
                        }
                    }
                }
                race_payload["events"][0]["data"]["transform"]["worker"]["person"] = {
                    "customFieldGroup": {"codeField": {"codeValue": i["pref_race_eth"]}}
                    # TODO: try single value, if that works, try sending multiple events at once
                }

                adp.post(
                    session=adp_client,
                    endpoint=WORKER_ENDPOINT,
                    subresource="person.custom-field.code",
                    verb="change",
                    payload=race_payload,
                )

                print()
    print("SUCCESS!")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
