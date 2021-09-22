from io import StringIO
import json
import mysql.connector
import os
import os.path
import psycopg2

from datetime import datetime, tzinfo, timezone
from dateutil.parser import parse
from mysql.connector import connect

config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

mysqlConfig = config["mysql"]
pgConfig = config["postgres"]

def getTimeStamp(ts : str) -> str:
    # Truncate timestamps with more than 6 fractional second digits
    # because the python library cannot handle that.
    if len(ts) > 26:
        ts = ts[:26] + "Z"

    return parse(ts).isoformat(timespec="seconds")

def v2Parser(msg : dict) -> dict:
    x = {}
    x["version"] = 2
    x["appId"] = msg["app_id"]
    x["devId"] = msg["dev_id"]
    x["hwSerial"] = msg["hardware_serial"]
    x["port"] = msg["port"]
    x["payload"] = msg["payload_raw"]

    # Cannot store a datetime object in the summary object because the json serialiser chokes on it.
    # Found at least one message where the gateway time field was an empty string so catch the error
    # and fall back to using the time the message was received by the TTN server which can be a
    # couple of seconds later.
    try:
        x["time"] = parse(msg["metadata"]["gateways"][0]["time"]).isoformat(timespec="seconds")
    except:
        #print("Failed to parse time from gateways in message:")
        #print(json.dumps(msg))
        x["time"] = getTimeStamp(msg["metadata"]["time"])
        #print(json.dumps(x, indent=2))
        #exit(1)

    return x

def v3Parser(msg : dict) -> dict:
    x = {}
    x["version"] = 3
    x["appId"] = msg["end_device_ids"]["application_ids"]["application_id"]
    x["devId"] = msg["end_device_ids"]["device_id"]
    x["hwSerial"] = msg["end_device_ids"]["dev_eui"]
    if "f_port" in msg["uplink_message"]:
        x["port"] = msg["uplink_message"]["f_port"]
    
    if "frm_payload" in msg["uplink_message"]:
        x["payload"] = msg["uplink_message"]["frm_payload"]

    # Cannot store a datetime object in the summary object because the json serialiser chokes on it.    
    x["time"] = getTimeStamp(msg["received_at"])
    return x

offset = 0
batchSize = 10000

with connect(host=mysqlConfig['host'], port=mysqlConfig['port'], user=mysqlConfig['user'], password=mysqlConfig['password'], database=mysqlConfig['database']) as connection:
    with psycopg2.connect(f"host={pgConfig['host']} port={pgConfig['port']} dbname={pgConfig['database']} user={pgConfig['user']} password={pgConfig['password']}") as pg:

        while True:
            with connection.cursor() as cursor:
                print("#", end="", flush=True)

                cursor.execute("select uid, payload from RawData order by uid limit %s, %s", (offset, batchSize))

                rowCount = 0

                values = StringIO()

                allRows = list(cursor.fetchall())
                for row in allRows:
                    rowCount += 1
                    uid = row[0]
                    ttnMsg = json.loads(row[1])

                    if "dev_id" in ttnMsg:
                        msg = v2Parser(ttnMsg)
                    elif "end_device_ids" in ttnMsg:
                        msg = v3Parser(ttnMsg)
                    else:
                        print("\nCould not parse message: " + row[1])
                        continue

                    ts = datetime.fromisoformat(msg["time"])

                    line = "{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(uid, ts, msg["appId"], msg["devId"], msg["hwSerial"], msg["payload"], json.dumps(ttnMsg))
                    values.writelines((line, ))

                values.seek(0)

                with pg.cursor() as pgCursor:
                    pgCursor.copy_from(values, 'msgs', columns=('uid', 'ts', 'appid', 'devid', 'deveui', 'payload', 'msg'))

                values.close()

                cursor.close()

                pg.commit()

                if rowCount < batchSize:
                    break

                offset += rowCount
