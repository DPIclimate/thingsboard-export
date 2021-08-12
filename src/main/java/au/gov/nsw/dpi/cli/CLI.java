package au.gov.nsw.dpi.cli;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.TimePageLink;

import au.gov.nsw.dpi.model.DeviceInfo;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Export timeseries data from ThingsBoard.
 */
@Command(name = "cli", version = "1.0", description = "Export timeseries data from ThingsBoard.")
public class CLI implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(CLI.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // ThingsBoard REST client library.
    private static RestClient rc;

    // 1970 epoch representation of the first and last times we want messages from, in milliseconds.
    // These will be zero if not set using --to and --from.
    private long from;
    private long to;

    /**
     * Replace anything except alpha-numeric characters and underscores with an underscore. Collapse multiple underscores to a single character. Remove trailing underscores.
     *
     * <p>This method is used to make device and channel names suitable for use as directory and filenames.</p>
     *
     * @param str the String to sanitise.
     * @return the sanitised String.
     */
    private static String sanitiseString(final String str) {
        return str.replaceAll("\\W", "_").replaceAll("_+", "_").replaceFirst("_$", "");
    }

    private Path getDeviceDirectory(final DeviceInfo flSeen) throws Exception {
        return dir.resolve(flSeen.readingsPrefix);
    }

    private void createDeviceSummaryDirectory(final DeviceInfo flSeen) throws Exception {
        Files.createDirectories(getDeviceDirectory(flSeen));
    }

    /**
     * Writes a JSON file with information about the device such as the first and
     * last telemetry times and a list of exported timeseries filenames.
     *
     * @param deviceInfo the {@link DeviceInfo} object to be written.
     * @throws Exception if there is an error writing the file.
     */
    private void writeDeviceSummary(final DeviceInfo deviceInfo) throws Exception {
        final Path outputName;
        if (infoOnly) {
            // Write the info json files directly to the output directory when in
            // info only mode.
            outputName = dir.resolve(deviceInfo.readingsPrefix + ".json");
        } else {
            createDeviceSummaryDirectory(deviceInfo);
            outputName = getDeviceDirectory(deviceInfo).resolve(deviceInfo.readingsPrefix + ".json");
        }

        // Make it easier to recognise invalid first or last seen values.
        final long now = System.currentTimeMillis();
        if (deviceInfo.getFrom() > now) {
            deviceInfo.setFrom(0);
            deviceInfo.fromReadable = "Not available";
        }

        if (deviceInfo.getTo() < 1) {
            deviceInfo.toReadable = "Not available";
        }

        try (BufferedWriter br = Files.newBufferedWriter(outputName, StandardCharsets.UTF_8);) {
            br.write(deviceInfo.toString());
        }
    }

    /**
     * Write a summary info file and timeseries CSV or JSON files for the given device.
     *
     * @param device the device whose data is to be exported.
     * @throws Exception if an error occurs reading from ThingsBoard or writing files.
     */
    private void exportDevice(final Device device) throws Exception {
        final DeviceInfo devInfo = new DeviceInfo();
        devInfo.tbDevName = device.getName();
        devInfo.tbDevId = device.getId().getId().toString();

        logger.info("Processing device {}", device.getName());

        // Trim is early on to remove leading/trailing whitespace from our device names.
        devInfo.readingsPrefix = sanitiseString(device.getName().trim());

        final List<String> timeseriesKeys;
        if (keyNames != null && keyNames.length > 0) {
            timeseriesKeys = new ArrayList<>(keyNames.length);
            for (final String k : keyNames) {
                timeseriesKeys.add(k);
            }
        } else {
            // Export all timeseries keys if no list of keys was provided.
            timeseriesKeys = rc.getTimeseriesKeys(device.getId());
        }

        // Set the DeviceInfo to and from to values that ensure they will be out of normal
        // bounds and get set from any timeseries entries.
        devInfo.setFrom(Long.MAX_VALUE);
        devInfo.setTo(0);

        // Move from back by 1 ms because the ThingsBoard query seems to be > from, not >= from.
        if (from > 0) {
            from--;
        }

        if (to < 1) {
            to = Long.MAX_VALUE;
        }

        if ( ! infoOnly) {
            createDeviceSummaryDirectory(devInfo);
        }

        if (infoOnly) {
            // When only writing the device summary file, use getLatestTimeseries to get the
            // most recent message time. Leave the earliest message time undefined because
            // that requires fetching every message. The device creation time cannot be used
            // because it seems to be unreliable.
            final List<TsKvEntry> tsl = rc.getLatestTimeseries(device.getId(), timeseriesKeys);
            if (tsl != null) {
                // Check all key entries in case they don't all share the same timestamp.
                tsl.stream().forEach(tskv -> {
                    final long x = tskv.getTs();
                    if (x > devInfo.getTo()) {
                        devInfo.setTo(x);
                    }
                });
            }
        } else {
            if (jsonTs) {
                exportToJSON(device, devInfo, timeseriesKeys, from, to);
            } else {
                for (final String k : timeseriesKeys) {
                    exportKeyToCSV(device, devInfo, k, from, to);
                }
            }
        }

        writeDeviceSummary(devInfo);
    }

    /**
     * Export a set of keys in a format suitable to push to ThingsBoard via the API.
     *
     * @param device the device of interest.
     * @param devInfo a {@link DeviceInfo} object which is updated with earliest/latest timestamps.
     * @param keys a list of timeseries keys to export.
     * @param earliest the earliest timestamp to export.
     * @param latest the latest timestamp to export.
     */
    private void exportToJSON(final Device device, final DeviceInfo devInfo, final List<String> keys, final long earliest, final long latest) {
        final int limit = 10000;

        try {
            final Path outputName = getDeviceDirectory(devInfo).resolve("timeseries.json");
            final BufferedWriter br = infoOnly ? null : Files.newBufferedWriter(outputName, StandardCharsets.UTF_8);

            long end = latest;

            final Map<Long, List<TsKvEntry>> tsMap = new HashMap<>();

            while (true) {
                logger.debug("Looking for messages between {} and {}", sdf.format(new Date(earliest)), sdf.format(new Date(end)));

                final TimePageLink pageLink = new TimePageLink(limit, earliest, end);
                final List<TsKvEntry> tsl = rc.getTimeseries(device.getId(), keys, 0L, Aggregation.NONE, pageLink);

                // getTimeseries returns results with the latest reading at the head of the list - element 0, and the earliest
                // reading at the tail, ie size() - 1.
                final int sz = tsl.size();
                logger.debug("Received {} messages", sz);

                if (sz > 0) {
                    final long ts1 = tsl.get(0).getTs(); // End of window - latest date
                    final long ts2 = tsl.get(sz-1).getTs(); // Start of window - earliest date

                    logger.debug("Date range of messages is {} to {}", sdf.format(new Date(ts2)), sdf.format(new Date(ts1)));

                    if (ts1 > devInfo.getTo()) {
                        devInfo.setTo(ts1);
                    }

                    if (ts2 < devInfo.getFrom()) {
                        devInfo.setFrom(ts2);
                    }

                    for (final TsKvEntry tskv : tsl) {
                        List<TsKvEntry> values;
                        if (tsMap.containsKey(tskv.getTs())) {
                            values = tsMap.get(tskv.getTs());
                        } else {
                            values = new ArrayList<>();
                            tsMap.put(tskv.getTs(), values);
                        }

                        if ( ! StringUtils.isEmpty(tskv.getValueAsString())) {
                            values.add(tskv);
                        }
                    }

                    // Don't start on the same reading in the next batch.
                    end = ts2 - 1;
                }

                // Less than a full set of readings means we now have the earliest reading
                // and can finish up for this field.
                if (sz < limit) {
                    break;
                }
            }

            if (tsMap.size() > 0) {
                // A set of longs will get naturally sorted from smallest to largest, ie
                // earliest timestamp to latest.
                final List<Long> timestamps = tsMap.keySet().stream().sorted().collect(Collectors.toList());
                if ( ! ascending) {
                    // So if the ascending flag is not set this natural order must be reversed.
                    Collections.reverse(timestamps);
                }

                br.write("[\n");
                boolean firstTs = true;
                for (final long ts : timestamps) {
                    if ( ! firstTs) {
                        br.write(",\n");
                    }
                    firstTs = false;

                    final List<TsKvEntry> values = tsMap.get(ts);
                    if (values.size() > 0) {
                        br.write("{\n");
                        br.write(String.format("    \"ts\": %d,\n    \"values\": {\n", ts));
                        boolean firstValue = true;
                        for (final TsKvEntry entry : values) {
                            if ( ! firstValue) {
                                br.write(",\n");
                            }
                            firstValue = false;
                            br.write(String.format("        \"%s\": %s", entry.getKey(), entry.getValueAsString()));
                        }
                        br.write("}}\n");
                    }
                }
                br.write("]\n");
            }

            br.flush();
            br.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Export a single key to a csv file.
     *
     * @param device the device of interest.
     * @param devInfo a {@link DeviceInfo} object which is updated with earliest/latest timestamps.
     * @param key the timeseries key to export.
     * @param earliest the earliest timestamp to export.
     * @param latest the latest timestamp to export.
     */
    private void exportKeyToCSV(final Device device, final DeviceInfo devInfo, final String key, final long earliest, final long latest) {
        final int limit = 10000;
        final String csvFormat = "%s,%s\r\n";

        logger.info("Processing key {} ", key);

        String z = devInfo.readingsPrefix + "_" + key;
        z = sanitiseString(z.trim());

        try {
            final Path outputName = getDeviceDirectory(devInfo).resolve(z + ".csv");
            final BufferedWriter br = infoOnly ? null : Files.newBufferedWriter(outputName, StandardCharsets.UTF_8);
            if ( ! infoOnly) {
                devInfo.fieldToFilename.put(key,  outputName.getFileName().toString());
            }

            long end = latest;
            final List<TsKvEntry> allEntries = new ArrayList<>();

            while (true) {
                logger.debug("Looking for messages between {} and {}", sdf.format(new Date(earliest)), sdf.format(new Date(end)));

                final TimePageLink pageLink = new TimePageLink(limit, earliest, end);
                final List<String> keys = new ArrayList<>(1);
                keys.add(key);
                final List<TsKvEntry> tsl = rc.getTimeseries(device.getId(), keys, 0L, Aggregation.NONE, pageLink);

                // getTimeseries returns results with the latest reading at the head of the list - element 0, and the earliest
                // reading at the tail, ie size() - 1.
                final int sz = tsl.size();
                logger.debug("Received {} messages", sz);

                if (sz > 0) {
                    final long ts1 = tsl.get(0).getTs(); // End of window - latest date
                    final long ts2 = tsl.get(sz-1).getTs(); // Start of window - earliest date

                    logger.debug("Date range of messages is {} to {}", sdf.format(new Date(ts2)), sdf.format(new Date(ts1)));

                    if (ts1 > devInfo.getTo()) {
                        devInfo.setTo(ts1);
                    }

                    if (ts2 < devInfo.getFrom()) {
                        devInfo.setFrom(ts2);
                    }

                    if ( ! infoOnly) {
                        allEntries.addAll(tsl);
                    }

                    // Don't start on the same reading in the next batch.
                    end = ts2 - 1;
                }

                // Less than a full set of readings means we now have the earliest reading
                // and can finish up for this field.
                if (sz < limit) {
                    break;
                }
            }

            if (infoOnly) {
                return;
            }

            if (ascending) {
                Collections.reverse(allEntries);
            }

            for (final TsKvEntry tskv : allEntries) {
                final String value = tskv.getValueAsString();
                if ( ! value.trim().isEmpty()) {
                    String ts;
                    if (humanReadableDates) {
                        ts = sdf.format(tskv.getTs());
                    } else {
                        ts = Long.toString(tskv.getTs());
                    }
                    br.write(String.format(csvFormat, ts, value));
                }
            }

            br.flush();
            br.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Option(names = { "-n", "--devname" }, description = "the ThingsBoard name for the device, may be a comma separated list of device names")
    private String[] deviceNamesArray;

    @Option(names = { "--devnamefile" }, description = "read devices names from the given file with one device name per line")
    private Path deviceNamesFile;

    @Option(names = { "-k", "--keys" }, description = "the ThingsBoard timeseries key names to export, may be a comma separated list of multiple field names")
    private String[] keyNames;

    @Option(names = { "-h", "--url" }, defaultValue="ThingsBoard.farmdecisiontech.net.au", description = "the ThingsBoard hostname; include the port if necessary such as some.host:9090")
    private String host;

    @Option(names = { "-u", "--user" }, required = true, description = "the ThingsBoard username")
    private String user;

    @Option(names = { "-p", "--password" }, required = true, description = "the ThingsBoard password")
    private String password;

    @Option(names = { "-i", "--info" }, description = "only write the info file for each named device")
    private boolean infoOnly;

    @Option(names = { "-j", "--json" }, description = "write the timeseries data in a format suitable for use with ThingsBoard saveEntityTelemetry REST call")
    private boolean jsonTs;

    @Option(names = { "-hr", "--human-readable" }, description = "write dates in human-readable format")
    private boolean humanReadableDates;

    @Option(names = { "-a", "--ascending" }, description = "write timeseries data from earliest to latest")
    private boolean ascending;

    @Option(names = { "-d", "--dir" }, defaultValue=".", description = "the directory to write the files into")
    private Path dir;

    @Option(names = { "-f", "--from" }, description = "the earliest timeseries entry to retrieve in UNIX-epoch-encoding, ie a long value")
    private String fromStr;

    @Option(names = { "-t", "--to" }, description = "the latest timeseries entry to retrieve in UNIX-epoch-encoding, ie a long value")
    private String toStr;

    // This gets populated from either deviceNamesArray or deviceNamesFile.
    private final List<String> deviceNamesList = new ArrayList<>();

    /**
     * picocli calls this method after parsing the arguments. Program logic starts here.
     */
    @Override
    public Integer call() throws Exception {
        sdf.setTimeZone(TimeZone.getTimeZone("AEST"));

        //
        // A few kludges here.
        //
        // So that date/time args do not have to be surrounded by quotes we want to accept
        // values such as 2021-04-12T23:15:32. But the format for SimpleDateFormat
        // cannot have literal characters in it so it cannot handle the 'T'.
        //
        // Replacing the 'T' with ' ' allows the use of the same date formatter as is used for
        // writing timestamps.
        //
        // Then to get the expected results we adjust the start and end dates to
        // 1 ms before/after the requested timestamp seconds.
        //
        if (StringUtils.isEmpty(fromStr)) {
            from = 0;
        } else {
            try {
                from = Long.parseLong(fromStr);
            } catch (final NumberFormatException e) {
                from = sdf.parse(fromStr.replace("T", " ")).getTime();
            }
        }

        if (StringUtils.isEmpty(toStr)) {
            to = 0;
        } else {
            try {
                to = Long.parseLong(toStr);
            } catch (final NumberFormatException e) {
                to = sdf.parse(toStr.replace("T", " ")).getTime();
                // Include all the milliseconds of the supplied timestamp.
                to += 999;
            }
        }

        try {
            if (deviceNamesFile != null) {
                if (deviceNamesArray != null) {
                    System.err.println("-n and --devnamefile are mutually exclusive");
                    return 1;
                }
                if ( ! (Files.exists(deviceNamesFile) && Files.isRegularFile(deviceNamesFile) && Files.isReadable(deviceNamesFile))) {
                    System.err.println("Cannot read file " + deviceNamesFile.toString());
                    return 1;
                }

                final Stream<String> deviceNameStream = Files.lines(deviceNamesFile);
                deviceNameStream.forEach(n -> deviceNamesList.add(n));
                deviceNameStream.close();
            }

            if (deviceNamesArray != null) {
                // Already checked for mutually exclusive args error above.
                for (final String n : deviceNamesArray) {
                    deviceNamesList.add(n);
                }
            }

            rc = new RestClient("https://" + host);
            rc.login(user, password);

            for (final String n : deviceNamesList) {
                try {
                    final Optional<Device> dev = rc.findDevice(n);
                    dev.ifPresent(d -> {
                        try {
                            exportDevice(d);
                        } catch (final Exception e) {
                            e.printStackTrace();
                        }
                    });
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            };

            rc.logout();

        } catch (final Exception e) {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

	public static void main(final String[] args) {
	    final int rc = new CommandLine(new CLI()).execute(args);
	    System.exit(rc);
	}
}
