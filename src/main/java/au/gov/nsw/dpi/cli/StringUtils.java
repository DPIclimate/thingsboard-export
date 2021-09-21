package au.gov.nsw.dpi.cli;

public class StringUtils {
    public static final boolean isEmpty(final String str) {
        return str == null  || str.trim().isEmpty();
    }

    public static final boolean isNotEmpty(final String str) {
        return ! isEmpty(str);
    }
}
