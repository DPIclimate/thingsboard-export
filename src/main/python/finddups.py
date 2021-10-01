import json
import mysql.connector
import os
import os.path
import psycopg2

from collections import deque
from datetime import datetime, tzinfo, timezone
from dateutil.parser import parse
from mysql.connector import connect

config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

postgresCfg = config["postgres"]

batchSize = 10000

lastMsg = {}


def getPostgresConnection():
    return psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"])


def getNextBatch(cursor, deveui, offset):
    cursor.execute("select uid, msg from msgs where deveui = %s AND (TO_DATE(msg->'metadata'->>'time', 'YYYY-MM-DDTHH:MI:SS') >= (NOW() - interval '7 days')) order by uid limit %s offset %s", (deveui, batchSize, offset))
    result = cursor.fetchall()
    return result

def findDupes(conn, deveui):
    dupCount = 0
    offset = 0

    # The deque will only hold the n most recent messages. Adding a new unique
    # message will remove the oldest one from the other end of the queue.
    recentMsgs = deque(maxlen=20)

    while True:
        with conn.cursor() as cursor:
            print("#", end="", flush=True)
            i = 0
            for (uid, msg) in getNextBatch(cursor, deveui, offset):
                i += 1
                if msg not in recentMsgs:
                    recentMsgs.append(msg)
                    continue

                dupCount = dupCount + 1

                with conn.cursor() as updateCursor:
                    updateCursor.execute("update msgs set (ignore, reason) = (%s, 'Duplicate in RawData') where uid = %s", (True, uid))

            conn.commit()

            offset += i
            if i < batchSize:
                break

    print("")
    print(f"Found {dupCount} duplicates.")


with getPostgresConnection() as conn:
    with conn.cursor() as devQry:
        devQry.execute("select distinct(deveui) from msgs")
        for (deveui, ) in devQry.fetchall():
            print(deveui)
            findDupes(conn, deveui)
