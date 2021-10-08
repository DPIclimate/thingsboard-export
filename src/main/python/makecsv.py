import argparse
import json
import os
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", help="the directory to write the files into", default=".", metavar="directory", dest="output_dir")
    parser.add_argument("-m", help="the file to load the timeseries from", metavar="filename", required=True, dest="payloads_json")
    parser.add_argument("-n", help="the ubidots device name", metavar="name", required=True, dest="device_name")

    args = parser.parse_args()

    s = os.path.splitext(os.path.basename(args.payloads_json))

    # Need this because the Java tool replaces - with _ . I think I did this because
    # ttnv3 devices cannot have - in their ids. But the tool doesn't create ttn devices
    # so I'm not sure why I felt it was necessary.
    underscored_dev_name = args.device_name.replace("-", "_")

    dest_dir = f"{args.output_dir}/{underscored_dev_name}"
    os.makedirs(dest_dir, exist_ok=True)

    dev_info = {"tbDevName": args.device_name, "tbDevId": "ignored", "devEui": "ignored", "readingsPrefix": underscored_dev_name, "fieldToFilename": {} }

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
    with open(args.payloads_json, "r") as payloads_file:
        msgs = json.load(payloads_file)

    # Load that array of dicts into a DataFrame.
    frame = pd.DataFrame(msgs)

    dev_info["from"] = str(frame.head(1)["ts"].values[0] * 1000)
    dev_info["to"] = str(frame.tail(1)["ts"].values[0] * 1000)

    # Write a CSV file for each column other than the "ts" column. The CSV file will have the
    # "ts" column and the other column, ready to be read by the migration tool.
    cols = frame.columns
    for a in cols:
        if a == "ts":
            continue

        fname = f"{dest_dir}/{underscored_dev_name}_{a}.csv"
        print(fname)
        frame.to_csv(fname, columns=["ts", a], header=False, index=False)

        dev_info["fieldToFilename"][a] = os.path.basename(fname)

    # Write the device summary JSON file.
    fname = f"{dest_dir}/{underscored_dev_name}.json"
    with open(fname, "w") as device_info_file:
        json.dump(dev_info, device_info_file, indent=2)

if __name__ == "__main__":
    # execute only if run as a script
    main()
