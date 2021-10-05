import json
import mysql.connector
import os
import os.path
import psycopg2

from collections import deque
from datetime import datetime, tzinfo, timezone, timedelta, date
from dateutil.parser import parse
from mysql.connector import connect

config = {}
with open("config.json", "r") as configFile:
    config = json.load(configFile)

postgresCfg = config["postgres"]

# Define prevous month here
currentDate = date(year=2021, month=10, day=1)
previousDate = currentDate - timedelta(days=32) # Current date minus just over a month

batchSize = 10000

lastMsg = {}


def getPostgresConnection():
    print("Connecting to database...")
    return psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"])


def getNextBatch(cursor, deveui, offset):
    cursor.execute("select uid, msg from msgs where deveui = %s and ((coalesce(msg->'metadata'->>'time', msg->'uplink_message'->>'recieved_at'))::timestamptz >= %s) order by uid limit %s offset %s", (deveui, previousDate, batchSize, offset))
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
        nDups = 0
        for (deveui, ) in devQry.fetchall():
            print(deveui)
            devDups = findDupes(conn, deveui)
            nDups += devDups
        print(f"A total of {nDups} were found.")


