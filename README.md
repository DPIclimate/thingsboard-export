# ThingsBoard timeseries export tool

This tool allows you to export timeseries data from ThingsBoard devices in either CSV or JSON format.

## Requirements

Building the API client library requires:
1. Java 1.8+
2. Maven 3.6.3+


## Basic use

```
$ mvn clean package
$ java -jar target\tbexport-1.0.0.jar -h thingsboard.host:9090 -u username -p password -n "device name"
```

### Other options

* `--devnamefile filename` to read device names from the given file
* `-k "key1,key2,..."` a comma-separated list of key names to export
* `-i` only write the device summary file
* `-j` write timeseries data in a JSON format suitable for use with the ThingsBoard timeseries writing API
* `-hr` write timestamps in human readable form rather than as a long value
* `-a` write timeseries data in ascending order
* `-d dirname` the output directory
* `-f timestamp` the earlist timestamp to export either as a long value or as yyyy-mm-ddThh:mm:ss
* `-t timestamp` the latest timestamp to export either as a long value or as yyyy-mm-ddThh:mm:ss
