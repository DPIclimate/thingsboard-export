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
    x["time"] = getTimeStamp(msg["metadata"]["time"])

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
    else:
        x["payload"] = ""

    x["time"] = getTimeStamp(msg["uplink_message"]["received_at"])

    return x

batchSize = 10000

with connect(host=mysqlConfig['host'], port=mysqlConfig['port'], user=mysqlConfig['user'], password=mysqlConfig['password'], database=mysqlConfig['database']) as raw_data:
    raw_data.autocommit = True
    raw_data_max_uid = 0
    with raw_data.cursor() as rd_cursor:
        rd_cursor.execute("select max(uid) from RawData")
        rs = rd_cursor.fetchone()
        raw_data_max_uid = rs[0]

    print(f"RawData max uid: {raw_data_max_uid}")

    with psycopg2.connect(f"host={pgConfig['host']} port={pgConfig['port']} dbname={pgConfig['database']} user={pgConfig['user']} password={pgConfig['password']}") as pg:
        msgs_max_uid = 0
        with pg.cursor() as pg_cursor:
            pg_cursor.execute("select max(uid) from msgs")
            rs = pg_cursor.fetchone()
            msgs_max_uid = rs[0]
            pg.commit() # close the txn opened by the query

        print(f"msgs    max uid: {msgs_max_uid}")

        while msgs_max_uid < raw_data_max_uid:
            with raw_data.cursor() as cursor:
                print(f"Reading batch starting at uid greater than {msgs_max_uid}")

                cursor.execute("select uid, payload from RawData where uid > %s order by uid limit %s", (msgs_max_uid, batchSize))

                values = StringIO()

                allRows = list(cursor.fetchall())
                for row in allRows:
                    uid = row[0]
                    msgs_max_uid = uid
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
