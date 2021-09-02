import mysql.connector
from mysql.connector import connect
import argparse
import subprocess
import json
import sys

#
# This program assumes ttn-lw-migrate and ttn-lw-cli are in /usr/local/bin and ready to run.
# The -b flag can be used to use an alternative binary directory.
#
# See https://www.thethingsindustries.com/docs/getting-started/cli/installing-cli/ for
# instructions on configuring ttn-lw-cli.
#

MYSQL_DB="LORA-MQTT-BROKER"

def getAppKey(appId: str) -> str:
    """
    Reads the key for appId from the TTNApps table.

    Parameters:
    appId: the application id.

    Returns:
    the application key.
    """
    print("Retrieving TTN application key.")
    with connect(host=args.mysqlHost, user=args.mysqlUser, password=args.mysqlPassword, database=MYSQL_DB) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select appKey from TTNApps where appId = %s", (appId, ))
            result = cursor.fetchone()
            if (cursor.rowcount != 1):
                raise "X"

            return result[0]


def updateDeviceBroker() -> None:
    """Updates the appId and devId values in the DeviceBroker table so it has the TTN v3 values."""
    print("Updating DeviceBroker row.")
    with connect(host=args.mysqlHost, user=args.mysqlUser, password=args.mysqlPassword, database=MYSQL_DB) as connection:
        with connection.cursor() as cursor:
            cursor.execute("update DeviceBroker set appId=%s, devId=%s where appId=%s and devId=%s", (args.v3AppId, args.v3DevId, args.v2AppId, args.v2DevId))
            connection.commit()


def exportV2Device(appId: str, appKey: str, deviceId: str) -> dict:
    """Export a device from TTN v2. If doing the final migration the device keys are changed so the device cannot join via a TTN v2 server."""
    print("Exporting device from TTN v2.")
    dryRun = "--dry-run"
    if args.noDryRun:
        print("No --dry-run flag.")
        dryRun = ""

    result = subprocess.run(
        [args.binDir + "/ttn-lw-migrate", "device", deviceId, dryRun, "--source", "ttnv2", "--ttnv2.app-id", appId, "--ttnv2.app-access-key", appKey, "--ttnv2.frequency-plan-id", "AS_920_923_TTN_AU"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        result.check_returncode()

    return json.loads(result.stdout)

def importV3Device(dev: dict) -> None:
    """Import a device to TTN v3."""
    print("Importing device to TTN v3.")
    jsonStr = json.dumps(dev)
    result = subprocess.run(
        [args.binDir + "/ttn-lw-cli", "end-devices", "create", "--application-id", args.v3AppId],
        capture_output=True, text=True, input=jsonStr)

    if result.returncode != 0:
        print("stdout:", result.stdout)
        print("stderr:", result.stderr)

parser = argparse.ArgumentParser()
parser.add_argument("-dbh", help="Broker database hostname", default="localhost", metavar="hostname", dest="mysqlHost")
parser.add_argument("-dbu", help="Broker database username", metavar="username", dest="mysqlUser")
parser.add_argument("-dbp", help="Broker database password", metavar="password", dest="mysqlPassword")

parser.add_argument("-sa", help="TTN v2 application id", metavar="v2AppId", dest="v2AppId")
parser.add_argument("-sd", help="TTN v2 device id", metavar="v2DevId", dest="v2DevId")

parser.add_argument("-da", help="TTN v3 application id", metavar="v3AppId", dest="v3AppId")
parser.add_argument("-dd", help="TTN v3 device id", metavar="v3DevId", dest="v3DevId")
parser.add_argument("-dn", help="TTN v3 device name, defaults to the TTN v3 device id", metavar="v3DevName", dest="v3DevName")

parser.add_argument("-x", help="Export device from TTN v3 using the ttn-lw-migrate command", default=False, action="store_const", const=True, dest="exportFromV2")
parser.add_argument("-ndr", help="No --dry-run flag, the TTN v2 keys will be changed to prevent the v2 device from joining", default=False, action="store_const", const=True, dest="noDryRun")

parser.add_argument("-i", help="Import the  device to TTN v3 using the ttn-lw-cli command", default=False, action="store_const", const=True, dest="importToV3")
parser.add_argument("-b", help="Update the DeviceBroker table", default=False, action="store_const", const=True, dest="updateBroker")

parser.add_argument("-d", help="Directory containing TTN command line tools", default="/usr/local/bin", metavar="dir", dest="binDir")
parser.add_argument("-f", help="The filename to read or write device JSON information", metavar="file", dest="filename")

args = parser.parse_args()

if args.exportFromV2:
    if args.v2AppId == None or args.v2DevId == None:
        print("-x requires -sa and -sd")
        sys.exit(1)

# If the TTN v3 values were not provided for application id, device id, and device name
# then default them to the TTN v2 values.
if not args.v3AppId:
    args.v3AppId = args.v2AppId.replace("_", "-")

if not args.v3DevId:
    args.v3DevId = args.v2DevId.replace("_", "-")

if not args.v3DevName:
    args.v3DevName = args.v3DevId

dev = None
if args.exportFromV2:
    appKey = getAppKey(args.v2AppId)
    dev = exportV2Device(args.v2AppId, appKey, args.v2DevId)

    # Update the device id and name to the values requested for the TTN v3 device.
    dev["ids"]["device_id"] = args.v3DevId
    dev["name"] = args.v3DevName

    # Save dev to a JSON file is a filename has been given.
    if args.filename:
        with open(args.filename, "w") as jsonFile:
            json.dump(dev, jsonFile, indent=2)

# If dev is not set because no export was done, and an import or broker update
# has been requested then dev must be initialised from an existing JSON file.
if args.importToV3 or args.updateBroker:
    if not dev:
        if not args.filename:
            print("Either -x or -f must be used to provide the device JSON.")
            sys.exit(1)

        with open(args.filename, "r") as jsonFile:
            dev = json.load(jsonFile)

if args.importToV3:
    importV3Device(dev)

if args.updateBroker:
    updateDeviceBroker()
