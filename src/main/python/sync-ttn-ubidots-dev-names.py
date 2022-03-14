import json
import requests
import subprocess
import sys


config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

ttn_cmd = config["bindir"] + "/ttn-lw-cli"


def ubi_get_token(apiKey: str) -> str:
    headers = { "x-ubidots-apikey": apiKey }
    response = requests.post("https://industrial.api.ubidots.com/api/v1.6/auth/token/", headers=headers)

    if not response.ok:
        print(f"Failed to get ubidots token. {response.status_code}: {response.reason}")
        sys.exit(1)

    body = response.json()
    return body["token"]


def ubi_get_device(ubiToken: str, devId: str) -> dict:
    headers = { "X-Auth-Token": ubiToken }
    response = requests.get(f"https://industrial.api.ubidots.com/api/v1.6/devices/{devId}", headers=headers)

    if not response.ok:
        return None

    return response.json()


def ubi_update_data_source(ubiToken: str, id: str, info: dict) -> None:
    headers = { "X-Auth-Token": ubiToken, "Content-type": "application/json" }
    url = f"https://industrial.api.ubidots.com/api/v2.0/devices/{id}"
    print(url)
    response = requests.patch(url, headers=headers, json=info)
    print(f"PATCH response: {response.status_code}: {response.reason}")


def ttn_get_devices_for_app(appId: str) -> dict:
    """Get device info for the given TTN v3 application."""
    result = subprocess.run([ttn_cmd, "d", "search", appId, "--all"], capture_output=True, text=True)

    if result.returncode != 0:
        print("stderr:", result.stderr)
        sys.exit(1)

    return json.loads(result.stdout)


def ttn_get_device(appId: str, devId: str) -> dict:
    result = subprocess.run([ttn_cmd, "d", "get", appId, devId, "--name", "--description"], capture_output=True, text=True)

    if result.returncode != 0:
        print("stderr:", result.stderr)
        sys.exit(1)

    return json.loads(result.stdout)


ubi_token = ubi_get_token(config["ubidots"]["apikey"])

ttn_app_id = sys.argv[1]

print(f"Searching for devices in TTN application {ttn_app_id}")
ttn_devs = ttn_get_devices_for_app(ttn_app_id)
for d in ttn_devs:
    eui = d["ids"]["dev_eui"].lower()
    ttn_dev_id = d["ids"]["device_id"]
    ttn_dev_name = d["name"] if "name" in d else ttn_dev_id
    ttn_dev_desc = d["description"] if "description" in d else None

    print(f"Found device {ttn_dev_id} / {ttn_dev_name} / {ttn_dev_desc}")
    try:
        ubi_device = ubi_get_device(ubi_token, eui)
        if ubi_device == None:
            print("Device not in ubidots.")
            continue

        updated_fields = {}
        updated_fields["name"] = ttn_dev_name
        if ttn_dev_desc is not None:
            updated_fields["description"] = ttn_dev_desc

        """
        Setting device properties does not seem to be as simple as giving a dict of properties.
        See this JSON from a device where a property was set via the UI:

        "url": "https://industrial.api.ubidots.com/api/v1.6/datasources/616e258886f43b0269f9fadd",
        "context": {
            "test1": "value1",
            "_config": {
            "test1": {
                "text": "test1",
                "type": "text",
                "description": "Property description here."
            }
            }
        },
        "tags": [],
        """
        #updated_fields["properties"] = { "_source": "ttn", "_appId": ttn_app_id, "_devId": ttn_dev_id}

        print(json.dumps(updated_fields))
        print("=============")

        ubi_update_data_source(ubi_token, ubi_device["id"], updated_fields)

        ubi_device = ubi_get_device(ubi_token, eui)
        print(json.dumps(ubi_device, indent=2))

    except:
        print("Not found")
