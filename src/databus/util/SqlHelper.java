package databus.util;

import java.util.regex.Pattern;

import io.netty.util.internal.ThreadLocalRandom;

public class SqlHelper {
    
    
    public static long id(long unixTime, int serviceId) {
        return (unixTime << 32) | ((serviceId & serviceIdMask) << 22) | rand();
    }

    public static String replaceEscapeString(String sql) {
        if (null == sql) {
            return "";
        }
        String replace = BSLASH_PATTERN.matcher(sql).replaceAll("\\\\\\\\");
        replace = QUOTE_PATTERN.matcher(replace).replaceAll("\\\\'");
        return replace;
    }
    
    private static int rand() {
        return ThreadLocalRandom.current().nextInt(randomBound);
    }
    
    private static int randomBound = 1 << 22;
    private static int serviceIdMask = (1 << 10) - 1;
    
    protected final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    protected final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");
}
