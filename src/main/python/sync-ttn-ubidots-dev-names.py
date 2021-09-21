import json
import requests
import subprocess
import sys


config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

ttnCmd = config["bindir"] + "/ttn-lw-cli"

def ubiGetToken(apiKey: str) -> str:
    headers = { "x-ubidots-apikey": apiKey }
    response = requests.post("https://industrial.api.ubidots.com/api/v1.6/auth/token/", headers=headers)

    if not response.ok:
        print(f"Failed to get ubidots token. {response.status_code}: {response.reason}")
        sys.exit(1)

    body = response.json()
    return body["token"]

def ubiGetDevice(ubiToken: str, devId: str) -> dict:
    headers = { "X-Auth-Token": ubiToken }
    response = requests.get(f"https://industrial.api.ubidots.com/api/v1.6/devices/{devId}", headers=headers)

    if not response.ok:
        return None

    return response.json()

def ubiUpdateDataSource(ubiToken: str, id: str, info: dict) -> None:
    headers = { "X-Auth-Token": ubiToken, "Content-type": "application/json" }
    url = f"https://industrial.api.ubidots.com/api/v1.6/datasources/{id}"
    print(url)
    response = requests.patch(url, headers=headers, json=info)
    print(f"PATCH response: {response.status_code}: {response.reason}")


def ttnGetDevicesForApp(appId: str) -> dict:
    """Get device info for the given TTN v3 application."""
    result = subprocess.run([ttnCmd, "d", "search", appId, "--all"], capture_output=True, text=True)

    if result.returncode != 0:
        print("stderr:", result.stderr)
        sys.exit(1)

    return json.loads(result.stdout)


def ttnGetDevice(appId: str, devId: str) -> dict:
    result = subprocess.run([ttnCmd, "d", "get", appId, devId, "--name"], capture_output=True, text=True)

    if result.returncode != 0:
        print("stderr:", result.stderr)
        sys.exit(1)

    return json.loads(result.stdout)


ubiToken = ubiGetToken(config["ubiApiKey"])

ttnAppId = sys.argv[1]

print(f"Searching for devices in TTN application {ttnAppId}")
ttnDevs = ttnGetDevicesForApp(ttnAppId)
for d in ttnDevs:
    eui = d["ids"]["dev_eui"].lower()
    ttnDevId = d["ids"]["device_id"]
    ttnDevName = d["name"]

    print(f"Found device {ttnDevId} / {ttnDevName}")
    try:
        x = ubiGetDevice(ubiToken, eui)
        if x == None:
            print("Device not in ubidots.")
            continue

        u = {}
        u["name"] = ttnDevId
        u["description"] = ttnDevName
        u["context"] = { "source": "ttn", "appId": ttnAppId, "devId": ttnDevId}
        print(json.dumps(u))
        print("=============")

        ubiUpdateDataSource(ubiToken, x["id"], u)

        x = ubiGetDevice(ubiToken, eui)
        print(json.dumps(x, indent=2))

    except:
        print("Not found")
