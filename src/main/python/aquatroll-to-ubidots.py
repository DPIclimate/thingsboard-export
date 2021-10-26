from datetime import datetime, timedelta
import requests
import pandas as pd
import csv
import json


USER_NAME = "PierceMatt"
DWN_KEY = "83S2-GZYD-TW82"
UBIDOTS_TOKEN="BBAU-89jVfECLkxlQasPB7qBU71Z5y6L5uZ"


def get_latest_telemetry():
    # Get start and end date
    endDate = datetime.now()
    startDate = endDate - timedelta(hours=3)

    # Format dates for request
    endDate = endDate.strftime("%Y-%m-%d%%20%H:%M:%S")
    startDate = startDate.strftime("%Y-%m-%d%%20%H:%M:%S")

    rUrl = f"http://{USER_NAME}:{DWN_KEY}@data.macemeters.com/download?start-date={startDate}&end-date={endDate}"

    # Get the latest data from mace (returns .csv file)
    r = requests.get(rUrl)

    # Write response into a csv file
    with open("data.csv", "wb") as f:
        f.write(r.content)

    r.close()

    # Process csv file with headers and drop rows with na's
    df = pd.read_csv("data.csv", skiprows=10, names=["Date", "Temperature", "Salinity", "Specific Conducitivty", "Actual Conductivity", "TDS", 
        "RDO Saturation", "RDO Concentration", "Oxygen", "pH", "pH mV", "Chlorophyll Concentration", "Chlorophyll fluorescence",
        "ORP", "Battery"], index_col="Date", encoding="latin1").dropna()

    # Save dataframe as a parsed csv file
    df.to_csv("parsed_data.csv")


def get_ubidots_latest():
    # Get the lastest value timestamp from ubidots to prevent unneeded requests
    # Although ubidots wont double up identical values its good to skip values that are already loaded
    ubiUrl = f"https://industrial.api.ubidots.com/api/v1.6/variables/615d2b56f4c81a045728b872"
    r = requests.get(ubiUrl, headers={"X-Auth-Token": UBIDOTS_TOKEN})
    rJson = json.loads(r.text)
    lastTimestamp = rJson["last_value"]["timestamp"]
    r.close()
    return lastTimestamp


def csv_to_json(csvFile):
    lastTimestamp = get_ubidots_latest()

    # Looks at the parsed csv file and converts each value to a basic json object to sent to ubidots
    ubidotsDevIds = ["615d2a918c58282149135629", "615d2a98f4c81a0491300839", "615d2a9df4c81a049130083a", "615d2aad8c58282184a573dc", 
                    "615d2ab98c58282022c5ffdb", "615d2ac083763f5ce0ebec40", "615d2b068c58282184a573dd", "615d2b1483763f5d1abb6d37", 
                    "615d2b1cf4c81a049130083b", "615d2b228c58282184a573de", "615d2b2883763f5ce0ebec41", "615d2b3783763f5d1abb6d38", 
                    "615d2b448c5828214913562a", "615d2b4f8c5828210eaa36a6", "615d2b56f4c81a045728b872"]

    with open(csvFile, encoding="utf-8") as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            readingTime = datetime.strptime(row["Date"], "%Y/%m/%d %H:%M:%S")
            print(readingTime)
            readingTime = int(readingTime.timestamp())*1000
            if readingTime > lastTimestamp:
                print("Adding values ubidots...")
                for key, val, devId in zip(row.keys(), row.values(), ubidotsDevIds):
                    ubiUrl = f"https://industrial.api.ubidots.com/api/v1.6/variables/{devId}/values/"
                    if(key != "Date"):
                        varJson = {"value": float(val), "timestamp": readingTime}
                        r = requests.post(ubiUrl, headers={"X-Auth-Token": UBIDOTS_TOKEN}, data=varJson)
                        print(r.text)
            else:
                print("Value already in ubidots.")


def main():
    get_latest_telemetry()
    csv_to_json("parsed_data.csv")


if __name__ == '__main__':
    main()
