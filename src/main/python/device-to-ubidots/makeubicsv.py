import argparse
import json
import os
import pandas as pd


def make_csv(payloads_json, device_label, drop_cols=[], output_dir=".", split=True):
    s = os.path.splitext(os.path.basename(payloads_json))

    dest_dir = f"{output_dir}/{device_label}"
    os.makedirs(dest_dir, exist_ok=True)

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

    print("Payloads file name: ", payloads_json)
    with open(payloads_json, "r") as payloads_file:
        msgs = json.load(payloads_file, strict=False)

    # Load that array of dicts into a DataFrame.
    df = pd.DataFrame(msgs)

    # Add a human-readable timestamp column. Peclet says this is required.
    df['hr'] = pd.to_datetime(df.ts, utc=True, unit="ms")
    # Move that column to after the timestamp column
    df.insert(1, 'hr', df.pop('hr'))

    df.rename(columns={'ts':'timestamp','hr':'human_readable_timestamp'}, inplace=True)

    # Drop columns named with the -x arguments.
    if drop_cols is not None:
        for a in drop_cols:
            df.pop(a)

    fname = f"{dest_dir}/{device_label}.csv"

    if split:
        moisturefname = f"{dest_dir}/{device_label}-1.csv"
        moistureCols = [col for col in df.columns if "temperature" not in col.lower()]
        print(moistureCols)
        moisturedf = df[moistureCols].dropna()
        print(moisturedf.head())

        tempfname = f"{dest_dir}/{device_label}-2.csv"
        tempCols = [col for col in df.columns if "moisture" not in col.lower()]
        print(tempCols)
        tempDf = df[tempCols].dropna()
        print(tempDf.head())

        tempDf.to_csv(tempfname, header=True, index=False)
        moisturedf.to_csv(moisturefname, header=True, index=False)

    else:
        df.to_csv(fname, header=True, index=False)
        print(df.head())
        print("Number of rows: ", len(df))
        print(fname)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", help="the directory to write the files into", default=".", metavar="directory", dest="output_dir")
    parser.add_argument("-m", help="the file to load the timeseries from", metavar="filename", required=True, dest="payloads_json")
    parser.add_argument("-n", help="the ubidots device label", metavar="name", required=True, dest="device_label")
    parser.add_argument("-x", help="columns to drop, can be given multiple times", metavar="columns", required=False, dest="drop_cols", action="append")

    args = parser.parse_args()
    make_csv(args.payloads_json, args.device_label, args.drop_cols, args.output_dir)


if __name__ == "__main__":
    # execute only if run as a script
    main()
