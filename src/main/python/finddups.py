import json
import mysql.connector
import os
import os.path
import sys
import psycopg2

from collections import deque
from datetime import datetime, tzinfo, timezone, timedelta, date
from dateutil.parser import parse
from mysql.connector import connect

config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

postgresCfg = config["postgres"]

currentDate = date(year=2021, month=10, day=1)
previousDate = currentDate - timedelta(days=32)

batchSize = 10000

lastMsg = {}

def getPostgresConnection():
    print("Connecting to database...")
    return psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"])


def getNextBatch(cursor, deveui, uid):
    print("Getting next batch")
    cursor.execute("select uid, msg from msgs where deveui = %s and uid >= %s order by uid limit %s", (deveui, uid, batchSize))
    result = cursor.fetchall()
    return result


def findDupes(conn, deveui, minUid):
    dupCount = 0
    offset = 0

    # The deque will only hold the n most recent messages. Adding a new unique
    # message will remove the oldest one from the other end of the queue.
    recentMsgs = deque(maxlen=20)

    while True:
        with conn.cursor() as cursor:
            print("#", end="", flush=True)
            i = 0
            try:
                for (uid, msg) in getNextBatch(cursor, deveui, minUid):
                    i += 1
                    minUid = uid
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
            except Exception as e:
                print(e)
                sys.exit()

    print("")
    print(f"Found {dupCount} duplicates.")
    return dupCount


with getPostgresConnection() as conn:
    with conn.cursor() as devQry:
        devQry.execute("select deveui, min(uid) from msgs WHERE (ts >= %s) GROUP BY deveui", (previousDate,))
        nDups = 0
        for (deveui, minUid) in devQry.fetchall():
            print(deveui)
            dups = findDupes(conn, deveui, minUid)
            nDups += dups
        print(f"Found a total of {nDups} duplicates.")

