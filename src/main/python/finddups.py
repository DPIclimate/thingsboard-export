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

currentDate = datetime.now(timezone.utc)
previousDate = currentDate - timedelta(days=7)

batchSize = 10000


def get_postgres_connection():
    print("Connecting to database...")
    return psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"])


def get_next_batch(cursor, deveui, uid):
    cursor.execute("select uid, msg from msgs where deveui = %s and uid >= %s order by uid limit %s", (deveui, uid, batchSize))
    result = cursor.fetchall()
    return result


def find_dupes(conn, deveui, minUid):
    dupCount = 0

    # The deque will only hold the n most recent messages. Adding a new unique
    # message will remove the oldest one from the other end of the queue.
    recentMsgs = deque(maxlen=20)

    while True:
        with conn.cursor() as cursor:
            print("#", end="", flush=True)
            i = 0
            try:
                for i, (uid, msg) in enumerate(get_next_batch(cursor, deveui, minUid)):
                    minUid = uid
                    if msg not in recentMsgs:
                        recentMsgs.append(msg)
                        continue

                    dupCount = dupCount + 1

                    with conn.cursor() as updateCursor:
                        updateCursor.execute("update msgs set (ignore, reason) = (%s, 'Duplicate in RawData') where uid = %s", (True, uid))

                conn.commit()

                if i < batchSize:
                    break
            except Exception as e:
                print(e)
                sys.exit(1)

    print("")
    print(f"Found {dupCount} duplicates.")
    return dupCount


def dedup_by_device():
    with get_postgres_connection() as conn:
        with conn.cursor() as dev_qry:
            dev_qry.execute("select deveui, min(uid) from msgs WHERE (ts >= %s) GROUP BY deveui", (previousDate,))
            nDups = 0
            for (deveui, minUid) in dev_qry.fetchall():
                print(deveui)
                dups = find_dupes(conn, deveui, minUid)
                nDups += dups
            print(f"Found a total of {nDups} duplicates.")


def main():
    dedup_by_device()


if __name__ == "__main__":
    # execute only if run as a script
    main()

