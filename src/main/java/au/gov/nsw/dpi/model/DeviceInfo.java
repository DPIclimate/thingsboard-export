package au.gov.nsw.dpi.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DeviceInfo extends ModelBaseObject {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String tbDevName = "";
    public String tbDevId = "";
    //private long created = 0;
    private long from = 0;
    private long to = 0;
    //public String createdReadable = "";
    public String fromReadable = "";
    public String toReadable = "";
    public String readingsPrefix = "";
    public Map<String, String> fieldToFilename = new HashMap<>();

//    public long getCreated() {
//        return created;
//    }
//
//    public void setCreated(final long created) {
//        this.created = created;
//        createdReadable = sdf.format(new Date(created));
//    }

    public long getFrom() {
        return from;
    }

    public void setFrom(final long from) {
        this.from = from;
        fromReadable = sdf.format(new Date(from));
    }

    public long getTo() {
        return to;
    }

    public void setTo(final long to) {
        this.to = to;
        toReadable = sdf.format(new Date(to));
    }
}
