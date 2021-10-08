import json
import os
import pandas as pd
import sys

def main():
    payloadsJson = sys.argv[1]
    s = os.path.splitext(os.path.basename(payloadsJson))
    devName = s[0]

    # Need this because the Java tool replaces - with _ . I think I did this because
    # ttnv3 devices cannot have - in their ids. But the tool doesn't create ttn devices
    # so I'm not sure why I felt it was necessary.
    underscoredDevName = devName.replace("-", "_")

    os.makedirs(underscoredDevName, exist_ok=True)

    devInfo = {"tbDevName": devName, "tbDevId": "ignored", "devEui": "ignored", "readingsPrefix": underscoredDevName, "fieldToFilename": {} }

    # Load the file written by the payload decoder. This must be a JSON file of the form
    # [
    # {"ts":1571198733825,"x":"3.6","y":"24.65","z":"34.91"},
    # ...
    # {"ts":1571788201289,"x":"3.6","y":"23.58","z":"38.11"}
    # ]
    #
    # ie an array of objects, where each object has a key "ts" with an epoch format timestamp and
    # one or more key/value pairs. Numeric values may be in quotes but do not have to be.
    #
    with open(payloadsJson, "r") as payloadsFile:
        msgs = json.load(payloadsFile)

    # Load that array of dicts into a DataFrame.
    frame = pd.DataFrame(msgs)

    devInfo["from"] = str(frame.head(1)["ts"].values[0] * 1000)
    devInfo["to"] = str(frame.tail(1)["ts"].values[0] * 1000)

    # Write a CSV file for each column other than the "ts" column. The CSV file will have the
    # "ts" column and the other column, ready to be read by the migration tool.
    cols = frame.columns
    for a in cols:
        if a == "ts":
            continue

        fname = f"{underscoredDevName}/{underscoredDevName}_{a}.csv"
        print(fname)
        frame.to_csv(fname, columns=["ts", a], header=False, index=False)

        devInfo["fieldToFilename"][a] = os.path.basename(fname)

    # Write the device summary JSON file.
    fname = f"{underscoredDevName}/{underscoredDevName}.json"
    with open(fname, "w") as jsonFile:
        json.dump(devInfo, jsonFile, indent=2)

if __name__ == "__main__":
    # execute only if run as a script
    main()
