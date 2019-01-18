package databus.receiver.mysql;

import java.util.regex.Pattern;

/**
 * Created by Qu Chunhe on 2016-11-04.
 */
public class MysqlHelper {

    public static String quoteReplacement(String message) {
        String replace = BSLASH_PATTERN.matcher(message).replaceAll("\\\\\\\\");
        replace = QUOTE_PATTERN.matcher(replace).replaceAll("\\\\'");
        return replace;
    }

    private final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    private final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");
}
