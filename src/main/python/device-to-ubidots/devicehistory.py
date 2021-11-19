import json
import psycopg2
import mysql.connector
from mysql.connector import connect
from dotenv import load_dotenv
import datetime
import pandas as pd
import xlrd
import requests
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


def device_list_to_csv(config):
    # Gets a list of unique devices from the history table
    # Gets their start and end data and a count of messages
    # Duplicate messages are ignored

    pgConnection = postgres_connect(config["postgres"])

    with pgConnection as pgConn:
        with pgConn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT devid FROM msgs")
            deviceList = cursor.fetchall()
            stats = []
            for (devid, ) in deviceList:
                cursor.execute("SELECT min(ts), max(ts), COUNT(*) from msgs WHERE devid=%s and ignore='f'", (str(devid), ))
                summary = cursor.fetchall()
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
    df.to_csv("raw-summary.csv")
    print(df)


def get_ubidots_creation_date(ubiToken):
    # Uses the device summary table to get the ubidots creation date
    # Only runs devices that aren't migrated and there is a corresponding ubidots api label

    df = pd.read_excel("device-summary.xlsx")

    for i, devid in enumerate(df["v2 devid"]):
        device_label = df["ubidots api label"].iloc[i]
        migrated = df["transferred"].iloc[i]
        if not pd.isnull(device_label) and pd.isnull(migrated):
            ubi_url = f"https://industrial.api.ubidots.com/api/v1.6/devices/{device_label}/"
            r = requests.get(ubi_url, headers={"X-Auth-Token": ubiToken})
            r_json = json.loads(r.text)
            creation_date = datetime.datetime.strptime(r_json["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            creation_date = int(creation_date.timestamp())*1000
            print(devid, "\t", creation_date)



def get_raw_payloads(config):
    # Queries the history database 
    # Converts to json and then decodes the values

    # === Setup ===
    decoder_f_name = "soil-ict-enviropro80cm" # dont include file extension

    # For each device
    device_name = "enviro80cm-a0a" # On TTNv2
    output_f_name = "stoneleigh-enviropro80cm-a0a" # On TTNv3
    ubi_api_label = "001fa14645528962" # API label on ubidots
    to_date = 1631797857000 # Unix timestamp in sec

    # === End Setup ===
    to_date = to_date / 1000

    pgConnection = postgres_connect(config["postgres"])

    print("getting messages from history db...")
    with pgConnection as pgConn:
        with pgConn.cursor() as cursor:
            cursor.execute("select json_agg(msg) from (select uid, msg from msgs where devid = %s and ts < to_timestamp(%s) and ignore = 'f' order by uid) as x", (device_name, to_date, ))
            responses = cursor.fetchall()
            with open("payloads.json", "w", encoding="utf-8") as log:
                for res in responses:
                    json.dump(res[0], log)

    # Run the payloads.json through decoder
    print("decoding messages...")
    os.system(f"node decode.js decoders/{decoder_f_name}.js ./payloads.json > decoded/{output_f_name}.json")
                    
    print("converting to csv...")
    os.system(f"python3 makeubicsv.py -d decoded_csvs -m decoded/{output_f_name}.json -n {ubi_api_label}")

def main():
    config = get_config("config.json")
    #device_list_to_csv(config)

    #load_dotenv()
    #UBIDOTS_TOKEN = os.getenv("UBIDOTS_TOKEN")
    #get_ubidots_creation_date(UBIDOTS_TOKEN)

    get_raw_payloads(config)


if __name__ == "__main__":
    main()
