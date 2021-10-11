package au.gov.nsw.dpi.cli;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.TimePageLink;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.ubidots.ApiClient;
import com.ubidots.DataSource;
import com.ubidots.Variable;

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
    private static final SimpleDateFormat csvHrFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss XX");

    // ThingsBoard REST client library.
    private static RestClient rc = null;

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

    private Path getDeviceDirectory(final DeviceInfo devInfo) throws Exception {
        return dir.resolve(devInfo.readingsPrefix);
    }

    private void createDeviceSummaryDirectory(final DeviceInfo devInfo) throws Exception {
        Files.createDirectories(getDeviceDirectory(devInfo));
    }

    /**
     * Create a minimal DeviceInfo object from the given Device.
     *
     * @param device the ThingsBoard Device to create the DeviceInfo from.
     * @return the new DeviceInfo object.
     */
    private DeviceInfo createDeviceInfo(final Device device) {
        return createDeviceInfo(device.getName(), device.getId().getId().toString());
    }

    /**
     * Create a minimal DeviceInfo object from the given Device.
     *
     * @param tbDevName the name of the device.
     * @param tbDevId the ThingsBoard id of the device.
     * @return the new DeviceInfo object.
     */
    private DeviceInfo createDeviceInfo(final String tbDevName, final String tbDevId) {
        final DeviceInfo devInfo = new DeviceInfo();
        devInfo.tbDevName = tbDevName;
        devInfo.tbDevId = tbDevId;

        // Trim is early on to remove leading/trailing whitespace from our device names.
        devInfo.readingsPrefix = sanitiseString(tbDevName.trim());
        return devInfo;
    }

    /**
     * Writes a JSON file with information about the device such as the first and
     * last telemetry times and a list of exported timeseries filenames.
     *
     * @param devInfo the {@link DeviceInfo} object to be written.
     * @throws Exception if there is an error writing the file.
     */
    private void writeDeviceSummary(final DeviceInfo devInfo) throws Exception {
        final Path outputName;
        if (infoOnly) {
            // Write the info json files directly to the output directory when in
            // info only mode.
            outputName = dir.resolve(devInfo.readingsPrefix + ".json");
        } else {
            createDeviceSummaryDirectory(devInfo);
            outputName = getDeviceDirectory(devInfo).resolve(devInfo.readingsPrefix + ".json");
        }

        // Make it easier to recognise invalid first or last seen values.
        final long now = System.currentTimeMillis();
        if (devInfo.getFrom() > now) {
            devInfo.setFrom(0);
            devInfo.fromReadable = "Not available";
        }

        if (devInfo.getTo() < 1) {
            devInfo.toReadable = "Not available";
        }

        try (BufferedWriter br = Files.newBufferedWriter(outputName, StandardCharsets.UTF_8);) {
            br.write(devInfo.toString());
        }
    }

    /**
     * Write a summary info file and timeseries CSV or JSON files for the given device.
     *
     * @param device the device whose data is to be exported.
     * @throws Exception if an error occurs reading from ThingsBoard or writing files.
     */
    private void exportDevice(final Device device) throws Exception {
        logger.info("Exporting device {}", device.getName());

        final DeviceInfo devInfo = createDeviceInfo(device);

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
     * Retrieve all timeseries values for the given keys and date range.
     *
     * @param device the device of interest.
     * @param devInfo a {@link DeviceInfo} object which is updated with earliest/latest timestamps.
     * @param keys a list of timeseries keys to export.
     * @param earliest the earliest timestamp to export.
     * @param latest the latest timestamp to export.
     *
     * @return all timeseries values for the given keys and date range.
     */
    private Map<Long, List<TsKvEntry>> getTimeseriesEntries(final Device device, final DeviceInfo devInfo, final List<String> keys, final long earliest, final long latest) {
        final int limit = 10000;

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
                    if (StringUtils.isEmpty(tskv.getValueAsString())) {
                        continue;
                    }

                    final List<TsKvEntry> values;
                    if (tsMap.containsKey(tskv.getTs())) {
                        values = tsMap.get(tskv.getTs());
                    } else {
                        values = new ArrayList<>();
                        tsMap.put(tskv.getTs(), values);
                    }

                    values.add(tskv);
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

        return tsMap;
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
        try {
            final Path outputName = getDeviceDirectory(devInfo).resolve("timeseries.json");
            final BufferedWriter br = infoOnly ? null : Files.newBufferedWriter(outputName, StandardCharsets.UTF_8);

            final Map<Long, List<TsKvEntry>> tsMap = getTimeseriesEntries(device, devInfo, keys, earliest, latest);
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
                        ts = csvHrFormat.format(tskv.getTs());
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

    /**
     * Push timeseries data to ubidots from an exported device. This method expects the data
     * to be in the same format as export writes with a device summary JSON file and a
     * set of CSV files for the variables.
     *
     * @param device the name of the device. This is used to find the JSON and CSV files.
     * @throws Exception if there is an error.
     */
    private void migrateDevice(final String deviceName) throws Exception {
        logger.info("Migrating device {}", deviceName);

        // Create enough of a DeviceInfo object to get the directory where the json file can be read.
        DeviceInfo devInfo = createDeviceInfo(deviceName, "ignored");
        final Path outputName = getDeviceDirectory(devInfo).resolve(devInfo.readingsPrefix + ".json");

        // Read in the device info json to get the CSV filenames.
        final Gson gson = new Gson();
        try (FileReader fr = new FileReader(outputName.toFile())) {
            devInfo = gson.fromJson(fr, DeviceInfo.class);
        }

        final Map<String, String> ubidotsConfig = (Map<String, String>)config.get("ubidots");
        final String ubiApiKey = ubidotsConfig.get("apikey");
        final ApiClient u = new ApiClient(ubiApiKey);
        final DataSource[] existingDataSources = u.getDataSources();

        // The calls to Thread.sleep are to keep the rate of API calls to below
        // the ubidots-imposed limit of 4/second.

        Thread.sleep(1100);
        boolean devExists = false;
        DataSource dataSource = null;
        for (final var ds : existingDataSources) {
            if (devInfo.tbDevName.equalsIgnoreCase(ds.getName())) {
                logger.info("Device {} already exists in Ubidots", ds.getName());
                dataSource = ds;
                devExists = true;
                break;
            }
        }

        if ( ! devExists) {
            if (readOnly) {
                logger.info("[read-only, no-op] Creating device {} in Ubidots", devInfo.tbDevName.trim());
            } else {
                logger.info("Creating device {} in Ubidots", devInfo.tbDevName.trim());
                dataSource = u.createDataSource(devInfo.tbDevName.trim());
                Thread.sleep(1100);
            }
        }

        final Variable[] existingVars = dataSource.getVariables();
        Thread.sleep(1100);
        final Map<String, Variable> variables = new HashMap<>();
        for (final var v : existingVars) {
            variables.put(v.getName(), v);
        }

        final ExecutorService es = Executors.newFixedThreadPool(devInfo.fieldToFilename.size());
        for (final var varName : devInfo.fieldToFilename.keySet()) {
            Variable v = null;
            if (variables.containsKey(varName)) {
                logger.info("Variable {} already exists.", varName);
                v = variables.get(varName);
            } else {
                if (readOnly) {
                    logger.info("[read-only, no-op] Creating variable {}.", varName);
                } else {
                    logger.info("Creating variable {}.", varName);
                    v = dataSource.createVariable(varName);
                    Thread.sleep(1100);
                }
            }

            final DataSource fds = dataSource;
            final Variable fv = v;
            final Path csv = getDeviceDirectory(devInfo).resolve(devInfo.fieldToFilename.get(varName));
            logger.info("Loading values for variable {} from {}", varName, csv.toString());

            es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    try {
                        // Create and use a new ApiClient object so each thread gets its own API token
                        // rather than sharing one. This allows each thread to make 4 calls per second
                        // instead of all threads being limited to 4 calls a second in total.
                        final ApiClient apiClient = new ApiClient(ubiApiKey);

                        // Now the DataSource and Variable have to be fetched from the new ApiClient
                        // instance. The Variables found above cannot be used with this thread-local
                        // ApiClient instance.
                        final DataSource threadDataSource = apiClient.getDataSource(fds.getId());
                        final Variable[] threadVars = threadDataSource.getVariables();
                        Variable threadVariable = null;
                        for (final var z : threadVars) {
                            if (z.getName().equals(fv.getName())) {
                                threadVariable = z;
                                break;
                            }
                        }

                        if (threadVariable != null && Files.isReadable(csv) && Files.isRegularFile(csv)) {
                            final var lines = Files.readAllLines(csv);
                            final int lineCount = lines.size();
                            int linesLeft = lineCount;

                            if (readOnly) {
                                logger.info("[read-only, no-op] Read csv for variable {}", varName);
                                return true;
                            }

                            // i is the index into the lines from the csv file. It is incremented
                            // in the inner loop that adds the values to the array that is sent to ubidots.
                            int i = 0;
                            while (i < lineCount) {
                                // 200 values at a time to keep under the 10kb limit
                                // ubidots has for the http post body.
                                int limit = 200;
                                if (linesLeft < limit) {
                                    limit = linesLeft;
                                }

                                final var values = new double[limit];
                                final var timestamps = new long[limit];

                                // This inner loop prepares a chunk of timestamps and values to
                                // send in a single request.
                                for (int j = 0; j < limit; j++) {
                                    final var line = lines.get(i);
                                    final var cols = line.split(",");
                                    timestamps[j] = Long.parseLong(cols[0]);
                                    values[j] = Double.parseDouble(cols[1]);
                                    i++;
                                }

                                // p & q are only used to calculate a percentage complete figure
                                // for display to the user.
                                final int q = lineCount - linesLeft + limit;
                                final int p = (int)((float)q / (float)lineCount * 100.0f);

                                logger.info("Saving {} values for key {}. {}%", limit, varName, p);
                                threadVariable.saveValues(values, timestamps);
                                Thread.sleep(300);

                                linesLeft -= limit;
                            }

                            return true;
                        } else {
                            logger.warn("Could not read CSV file for key {}.", varName);
                        }
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }

                    return false;
                }
            });
        }

        es.shutdown();
        while ( ! es.isTerminated()) {
            es.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    /**
     * Push timeseries data to ubidots from an exported device. This method expects the data
     * to be in the format written by the TTN v3 payload formatter driver program.
     *
     * @param device the name of the device. This is used to find the JSON and CSV files.
     * @param valuesFile the JSON file to read the values from.
     * @throws Exception if there is an error.
     */
    private void migrateDevice(final String deviceName, final Path valuesFile) throws Exception {
        logger.info("Migrating device {}", deviceName);

        logger.info("Reading file {}", valuesFile.toString());

        final List<Long> timestampsList = new ArrayList<>();
        final Map<String, List<Double>> kvPairs = new HashMap<>();

        try (final JsonReader reader = new JsonReader(new FileReader(valuesFile.toString()))) {
            reader.beginArray();
            while (reader.hasNext()) {
                reader.beginObject();
                while (reader.hasNext()) {
                    final String key = reader.nextName();

                    if ("ts".equals(key)) {
                        final long ts = reader.nextLong();
                        timestampsList.add(ts);
                    } else {
                        final double value = reader.nextDouble();
                        List<Double> values = kvPairs.get(key);
                        if (values == null) {
                            values = new ArrayList<Double>();
                            kvPairs.put(key, values);
                        }
                        values.add(value);
                    }
                }
                reader.endObject();
            }
            reader.endArray();
        }

        logger.info("Read {} entries from file", timestampsList.size());

        final Map<String, String> ubidotsConfig = (Map<String, String>)config.get("ubidots");
        final String ubiApiKey = ubidotsConfig.get("apikey");
        final ApiClient u = new ApiClient(ubiApiKey);
        final DataSource[] existingDataSources = u.getDataSources();

        // The calls to Thread.sleep are to keep the rate of API calls to below
        // the ubidots-imposed limit of 4/second.

        Thread.sleep(1100);
        boolean devExists = false;
        DataSource dataSource = null;
        for (final var ds : existingDataSources) {
            if (deviceName.equalsIgnoreCase(ds.getName())) {
                logger.info("Device {} already exists in Ubidots", ds.getName());
                dataSource = ds;
                devExists = true;
                break;
            }
        }

        if ( ! devExists) {
            if (readOnly) {
                logger.info("[read-only, no-op] Creating device {} in Ubidots", deviceName);
            } else {
                logger.info("Creating device {} in Ubidots", deviceName);
                dataSource = u.createDataSource(deviceName);
                Thread.sleep(1100);
            }
        }

        final Variable[] existingVars = dataSource.getVariables();
        Thread.sleep(1100);
        final Map<String, Variable> variables = new HashMap<>();
        for (final var v : existingVars) {
            variables.put(v.getName(), v);
        }

        final ExecutorService es = Executors.newFixedThreadPool(kvPairs.size());

        for (final var varName : kvPairs.keySet()) {
            Variable v = null;
            if (variables.containsKey(varName)) {
                logger.info("Variable {} already exists.", varName);
                v = variables.get(varName);
            } else {
                if (readOnly) {
                    logger.info("[read-only, no-op] Creating variable {}.", varName);
                } else {
                    logger.info("Creating variable {}.", varName);
                    v = dataSource.createVariable(varName);
                    Thread.sleep(1100);
                }
            }

            // Make final copies of the mutable dataSource and v(ariable) objects
            // so the threaded code can read them.
            final DataSource fds = dataSource;
            final Variable fv = v;

            es.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    try {
                        // Create and use a new ApiClient object so each thread gets its own API token
                        // rather than sharing one. This allows each thread to make 4 calls per second
                        // instead of all threads being limited to 4 calls a second in total.
                        final ApiClient apiClient = new ApiClient(ubiApiKey);

                        // Now the DataSource and Variable have to be fetched from the new ApiClient
                        // instance. The Variables found above cannot be used with this thread-local
                        // ApiClient instance.
                        final DataSource threadDataSource = apiClient.getDataSource(fds.getId());
                        final Variable[] threadVars = threadDataSource.getVariables();
                        Variable threadVariable = null;
                        for (final var z : threadVars) {
                            if (z.getName().equals(fv.getName())) {
                                threadVariable = z;
                                break;
                            }
                        }

                        final int lineCount = timestampsList.size();
                        int linesLeft = lineCount;
                        final List<Double> valuesList = kvPairs.get(fv.getName());

                        if (readOnly) {
                            logger.info("[read-only, no-op] Reading List for variable {}, {} entries", varName, valuesList.size());
                            return true;
                        }

                        // i is the index into the Lists built from the JSON file. It is incremented
                        // in the inner loop that adds the values to the array that is sent to ubidots.
                        int i = 0;
                        while (i < lineCount) {
                            // 200 values at a time to keep under the 10kb limit
                            // ubidots has for the http post body.
                            int limit = 200;
                            if (linesLeft < limit) {
                                limit = linesLeft;
                            }

                            final var values = new double[limit];
                            final var timestamps = new long[limit];

                            // This inner loop prepares a chunk of timestamps and values to
                            // send in a single request.
                            for (int j = 0; j < limit; j++) {
                                timestamps[j] = timestampsList.get(i);
                                values[j] = valuesList.get(i);
                                i++;
                            }

                            // p & q are only used to calculate a percentage complete figure
                            // for display to the user.
                            final int q = lineCount - linesLeft + limit;
                            final int p = (int)((float)q / (float)lineCount * 100.0f);

                            logger.info("Saving {} values for key {}. {}%", limit, varName, p);
                            threadVariable.saveValues(values, timestamps);
                            Thread.sleep(300);

                            linesLeft -= limit;
                        }

                        return true;
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }

                    return false;
                }
            });
        }

        es.shutdown();
        while ( ! es.isTerminated()) {
            es.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Option(names = { "-n", "--devname" }, description = "the ThingsBoard name for the device, may be given multiple times")
    private String[] deviceNamesArray;

    @Option(names = { "--devnamefile" }, description = "read devices names from the given file with one device name per line")
    private Path deviceNamesFile;

    @Option(names = { "-k", "--keys" }, description = "the ThingsBoard timeseries key names to export, may be a comma separated list of multiple field names")
    private String[] keyNames;

    @Option(names = { "-h", "--url" }, defaultValue="thingsBoard.farmdecisiontech.net.au", description = "the ThingsBoard hostname; include the port if necessary such as some.host:9090")
    private String host;

    @Option(names = { "-u", "--user" }, description = "the ThingsBoard username")
    private String user;

    @Option(names = { "-p", "--password" }, description = "the ThingsBoard password")
    private String password;

    @Option(names = { "-i", "--info" }, description = "only write the info file for each named device")
    private boolean infoOnly;

    @Option(names = { "-j", "--json" }, description = "write the timeseries data in a format suitable for use with ThingsBoard saveEntityTelemetry REST call, or read Ubidots values from a JSON file")
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

    @Option(names = { "-c", "--config" }, description = "path to the JSON config file")
    private Path configJson;

    @Option(names = { "-m" }, description = "migrate timeseries to Ubidots using exported data for the named device")
    private boolean migrateDevice;

    @Option(names = { "--values" }, description = "path to the JSON values file")
    private Path valuesFile;

    @Option(names = { "-r" }, description = "read-only - when -m is used, only check if devices and variables exist and CSV files can be read")
    private boolean readOnly;

    // This gets populated from either deviceNamesArray or deviceNamesFile.
    private final List<String> deviceNamesList = new ArrayList<>();

    private Map<?, ?> config;

    /**
     * picocli calls this method after parsing the arguments. Program logic starts here.
     */
    @Override
    public Integer call() throws Exception {
        sdf.setTimeZone(TimeZone.getTimeZone("AEST"));

        if (configJson != null) {
            if ( ! (Files.exists(configJson) && Files.isRegularFile(configJson) && Files.isReadable(configJson))) {
                System.err.println("Cannot read file " + configJson.toString());
                return 1;
            }

            final Gson gson = new Gson();
            final Reader reader = Files.newBufferedReader(Paths.get(configJson.toString()));
            config = gson.fromJson(reader, Map.class);

            final Map<String, String> tbConfig = (Map<String, String>)config.get("thingsboard");
            if (StringUtils.isEmpty(host)) {
                host = tbConfig.get("host");
                final String port = tbConfig.get("port");
                if (StringUtils.isNotEmpty(port)) {
                    host = host + ":" + port;
                }
            }

            if (StringUtils.isEmpty(user)) {
                user = tbConfig.get("user");
            }

            if (StringUtils.isEmpty(password)) {
                password = tbConfig.get("password");
            }
        }

        if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password) || StringUtils.isEmpty(host)) {
            if ( ! migrateDevice) {
                CommandLine.usage(this, System.err);
                return -1;
            }
        }

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

        if ( ! migrateDevice) {
            rc = new RestClient("https://" + host);
            rc.login(user, password);
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

            for (final String n : deviceNamesList) {
                try {
                    if (migrateDevice) {
                        if (jsonTs) {
                            migrateDevice(n.trim(), valuesFile);
                        } else {
                            // Read from the device info JSON, CSV timeseries files.
                            // Don't trim this name?
                            migrateDevice(n);
                        }
                        continue;
                    }

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

            if (rc != null) {
                rc.logout();
            }

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
