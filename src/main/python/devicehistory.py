import json
import psycopg2
import mysql.connector
from mysql.connector import connect
import datetime
import pandas as pd
import os
import sys


def get_config(file):
    try: 
        with open(file, "r") as configFile:
            config = json.load(configFile)
            return config
    except FileNotFoundError:
        print(f"{file} not found")
        sys.exit(1)


def postgres_connect(pgConfig):
    return psycopg2.connect(host=pgConfig["host"], 
                            port=pgConfig["port"], 
                            user=pgConfig["user"], 
                            password=pgConfig["password"], 
                            dbname=pgConfig["database"])


def get_summary(cursor, devid):
    cursor.execute("SELECT min(ts), max(ts), COUNT(*) from msgs WHERE devid='{0}' and ignore='f'".format(str(devid)))
    return cursor.fetchall()


def main():
    config = get_config("config.json")
    pgConnection = postgres_connect(config["postgres"])
    with pgConnection as pgConn:
        with pgConn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT devid FROM msgs")
            stats = []
            for (devid, ) in cursor.fetchall():
                summary = get_summary(cursor, devid)
                deviceValues = []
                deviceValues.append(devid)
                for value in summary[0]:
                    if(isinstance(value, datetime.datetime)):
                        deviceValues.append(value.strftime("%d/%m/%Y %H:%M:%S"))
                    else:
                        deviceValues.append(value)
                stats.append(deviceValues)
                print(deviceValues)

    df = pd.DataFrame(stats, columns=["devid", "start_time", "end_time", "count"])
    df.to_csv("devices_summary.csv")
    print(df)


if __name__ == "__main__":
    main()
