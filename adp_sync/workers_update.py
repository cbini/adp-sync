import gzip
import json
import os
import pathlib
import traceback

import adp

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CERT_FILEPATH = os.getenv("CERT_FILEPATH")
KEY_FILEPATH = os.getenv("KEY_FILEPATH")
ADP_IMPORT_FILE = os.getenv("ADP_IMPORT_FILE")
ADP_EXPORT_FILE = os.getenv("ADP_EXPORT_FILE")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent

WORKER_ENDPOINT = "/events/hr/v1/worker"


def get_worker_item(
    worker, item_name, object_name="customFieldGroup", attr_name="stringFields"
):
    return next(
        iter(
            [
                item
                for item in worker[object_name].get(attr_name, {})
                if item["nameCode"]["codeValue"] == item_name
            ]
        ),
        {},
    )


def flatten_worker(worker):
    worker_flat = {}

    worker_flat["associateOID"] = worker["associateOID"]

    worker_flat["work_email"] = get_worker_item(
        worker=worker,
        item_name="Work E-mail",
        object_name="businessCommunication",
        attr_name="emails",
    )

    worker_flat["employee_number"] = get_worker_item(
        worker=worker, item_name="Employee Number"
    )

    worker_flat["wfm_badge_number"] = get_worker_item(
        worker=worker, item_name="WFMgr Badge Number"
    )

    worker_flat["wfm_trigger"] = get_worker_item(
        worker=worker, item_name="WFMgr Trigger"
    )

    return worker_flat


def get_event_payload(associate_oid, item_id, string_value):
    payload = {
        "data": {
            "eventContext": {
                "worker": {"associateOID": associate_oid},
            },
            "transform": {"worker": {}},
        }
    }

    if item_id == "Business":
        payload["data"]["transform"]["worker"]["businessCommunication"] = {
            "email": {"emailUri": string_value}
        }
    else:
        payload["data"]["eventContext"]["worker"]["customFieldGroup"] = {
            "stringField": {"itemID": item_id}
        }
        payload["data"]["transform"]["worker"]["customFieldGroup"] = {
            "stringField": {"stringValue": string_value}
        }

    return payload


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
    workers_export_flat = [w for w in map(flatten_worker, workers_export_data)]
    print("\tSUCCESS!")

    print("Processing ADP updates...")
    for i in import_data:
        # match db record to ADP record
        record_match = next(
            iter(
                [
                    w
                    for w in workers_export_flat
                    if w["associateOID"] == i["associate_oid"]
                ]
            ),
            None,
        )

        if record_match:
            # update work email if new
            if i["mail"] != record_match.get("work_email").get("emailUri"):
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('work_email').get('emailUri')} => {i['mail']}"
                )

                work_email_data = get_event_payload(
                    associate_oid=i["associate_oid"],
                    item_id="Business",
                    string_value=i["mail"],
                )

                try:
                    adp.post(
                        session=adp_client,
                        endpoint=WORKER_ENDPOINT,
                        subresource="business-communication.email",
                        verb="change",
                        payload={"events": [work_email_data]},
                    )
                except Exception as xc:
                    print(xc)
                    print(traceback.format_exc())

            # set up custom field event list
            custom_field_events = []

            # update employee number if missing
            if not record_match.get("employee_number").get("stringValue"):
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('employee_number').get('stringValue')}"
                    f" => {i['employee_number']}"
                )

                emp_num_data = get_event_payload(
                    associate_oid=i["associate_oid"],
                    item_id=record_match.get("employee_number").get("itemID"),
                    string_value=i["employee_number"],
                )

                custom_field_events.append(emp_num_data)

            # update wfm badge number (employee_number), if missing
            if not record_match.get("wfm_badge_number").get("stringValue"):
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('wfm_badge_number').get('stringValue')}"
                    f" => {i['employee_number']}"
                )

                wfm_badge_data = get_event_payload(
                    associate_oid=i["associate_oid"],
                    item_id=record_match.get("wfm_badge_number").get("itemID"),
                    string_value=i["employee_number"],
                )

                custom_field_events.append(wfm_badge_data)

            # update wfm trigger if not null
            if i["wfm_trigger"]:
                print(
                    f"{i['employee_number']}"
                    f"\t{record_match.get('wfm_trigger').get('stringValue')}"
                    f" => {i['wfm_trigger']}"
                )

                wfm_trigger_data = get_event_payload(
                    associate_oid=i["associate_oid"],
                    item_id=record_match.get("wfm_trigger").get("itemID"),
                    string_value=i["wfm_trigger"],
                )

                custom_field_events.append(wfm_trigger_data)

            # post all custom field events
            if custom_field_events:
                try:
                    adp.post(
                        session=adp_client,
                        endpoint=WORKER_ENDPOINT,
                        subresource="custom-field.string",
                        verb="change",
                        payload={"events": custom_field_events},
                    )
                except Exception as xc:
                    print(xc)
                    print(traceback.format_exc())

    print("SUCCESS!")


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
