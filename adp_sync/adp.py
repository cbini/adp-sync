import traceback

import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

SERVICE_ROOT = "https://api.adp.com"


def authorize(client_id, client_secret, cert_filepath, key_filepath):
    # instantiate ADP client
    token_url = f"https://accounts.adp.com/auth/oauth/v2/token"
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    session = OAuth2Session(client=client)
    session.cert = (cert_filepath, key_filepath)

    # authorize ADP client
    token_dict = session.fetch_token(token_url=token_url, auth=auth)
    access_token = token_dict.get("access_token")
    session.headers["Authorization"] = f"Bearer {access_token}"

    return session


def get_record(session, endpoint, id, querystring={}):
    url = f"{SERVICE_ROOT}{endpoint}/{id}"
    r = session.get(url, params=querystring)
    if r.status_code == 200:
        return r.json()
    else:
        r.raise_for_status()


def get_paginated_records(session, endpoint, querystring={}):
    url = f"{SERVICE_ROOT}{endpoint}"
    querystring["$skip"] = querystring.get("$skip", 0)
    all_data = []
    while True:
        r = session.get(url, params=querystring)
        if r.status_code == 204:
            break
        if r.status_code == 200:
            data = r.json()
            all_data.extend(data.get(endpoint.split("/")[-1]))
            querystring["$skip"] += 50
        else:
            r.raise_for_status()
    return all_data


def post(session, endpoint, subresource, verb, payload):
    url = f"{SERVICE_ROOT}{endpoint}.{subresource}.{verb}"
    try:
        r = session.post(url, json=payload)
        r.raise_for_status()
    except requests.exceptions.HTTPError:
        resource_messages = r.json().get("confirmMessage").get("resourceMessages")
        process_messages = [m.get("processMessages") for m in resource_messages]
        formatted_message = f"\t{url}\n\t{payload}\n\n"
        for m in process_messages:
            print(f"message: {m}")
            formatted_message += (
                f"\t{r.status_code} - {r.reason}: "
                f"{m.get('userMessage').get('messageTxt')}"
            )
        print(formatted_message)
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
