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

postgresCfg = config["postgres"]

batchSize = 10000

lastMsg = {}


def getPostgresConnection():
    return psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"])


def getNextBatch(cursor, maxUid, offset):
    cursor.execute("select uid, deveui, msg from msgs order by uid where uid < %s limit %s offset %s", (maxUid, batchSize, offset))
    result = cursor.fetchall()
    return result


def populateMsgDict(maxUid):
    offset = 0
    lastBatchSize = 0
    
    print("Initialising last message map.")
    
    while True:
        with getPostgresConnection() as conn:
            print(".", end="", flush=True)
            with conn.cursor() as cursor:
                cursor.execute("select uid, deveui, msg from msgs where uid < %s and dup = 'f' order by uid limit %s offset %s", (maxUid + 1, batchSize, offset))
                result = cursor.fetchall()
                cursor.close()

                lastBatchSize = len(result)
                offset = offset + lastBatchSize

                for row in result:
                    uid = row[0]
                    deveui = row[1]
                    msg = row[2]

                    if not deveui in lastMsg:
                        lastMsg[deveui] = { "uid": uid, "msg": msg }
                        continue

                    lastMsg[deveui]["uid"] = uid
                    lastMsg[deveui]["msg"] = msg

        if lastBatchSize < batchSize:
            break

    print("")


def findDupes(minUid):
    dupCount = 0

    with psycopg2.connect(host=postgresCfg["host"], port=postgresCfg["port"], user=postgresCfg["user"], password=postgresCfg["password"], dbname=postgresCfg["database"]) as conn:
        while True:
            with conn.cursor() as cursor:
                print("#", end="", flush=True)

                result = getNextBatch(cursor)
                offset = offset + len(result)
                for row in result:
                    uid = row[0]
                    deveui = row[1]
                    msg = row[2]

                    if not deveui in lastMsg:
                        lastMsg[deveui] = { "uid": uid, "msg": msg}
                        continue

                    isDup = False
                    x = lastMsg[deveui]
                    if x["msg"] == msg:
                        dupCount = dupCount + 1
                        isDup = True
                    else:
                        lastMsg[deveui] = { "uid": uid, "msg": msg}

                    with conn.cursor() as updateCursor:
                        updateCursor.execute("update msgs set dup = %s where uid = %s", (isDup, uid))

                    lastMsg[deveui] = { "uid": uid, "msg": msg}


                conn.commit()

                if len(result) < batchSize:
                    break

    print(f"Found {dupCount} duplicates.")


populateMsgDict(2050543)
