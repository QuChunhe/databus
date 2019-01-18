package databus.receiver.cassandra;

import java.util.regex.Pattern;

/**
 * Created by Qu Chunhe on 2018-06-26.
 */
public class CassandraHelper {
    public static String replaceEscapeString(String cql) {
        if (null == cql) {
            return "";
        }
        return QUOTE_PATTERN.matcher(cql).replaceAll("''");
    }

    private final static Pattern QUOTE_PATTERN = Pattern.compile("'");
}
