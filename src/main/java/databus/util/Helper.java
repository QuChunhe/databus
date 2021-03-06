package databus.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.util.Properties;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Helper {

    public static Properties loadProperties(String file) {
        Properties properties= loadPropertiesWithoutExit(file);
        if (null == properties) {
            System.exit(1);
        }
        return properties;
    }

    public static Properties loadPropertiesWithoutExit(String file) {
        if ((null==file) || (file.length()==0)) {
            log.error("Configuration file is null!");
            return null;
        }
        Properties properties = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))){
            properties.load(reader);
        } catch (IOException e) {
            log.error("Can not load properties file "+ file, e);
            return null;
        }
        return properties;
    }

    public static long ipToInt(String ipAddr) throws UnknownHostException, DataFormatException {
        InetAddress inetAddress = InetAddress.getByName(ipAddr);
        byte[] parts = inetAddress.getAddress();
        if (parts.length != 4) {
            throw new DataFormatException(ipAddr+" length does't equal to 4");
        }
        long ip = 0;
        ip |= Byte.toUnsignedLong(parts[0]) << 24;
        ip |= Byte.toUnsignedLong(parts[1]) << 16;
        ip |= Byte.toUnsignedLong(parts[2]) << 8;
        ip |= Byte.toUnsignedLong(parts[3]);
     
        return ip;
    }

    public static ThreadPoolExecutor loadExecutor(int corePoolSize, int maximumPoolSize,
                                                  long keepAliveSeconds, int taskCapacity) {
        return new ThreadPoolExecutor(corePoolSize,
                                      maximumPoolSize,
                                      keepAliveSeconds,
                                      TimeUnit.SECONDS,
                                      new ArrayBlockingQueue<>(taskCapacity),
                                      new CallerWaitsPolicy());
    }

    public static String replaceEscapeString(String sql) {
        if (null == sql) {
            return "";
        }
        String replace = BSLASH_PATTERN.matcher(sql).replaceAll("\\\\\\\\");
        replace = QUOTE_PATTERN.matcher(replace).replaceAll("\\\\'");
        return replace;
    }

    public static String substring(String string, String splitter) {
        int position = string.indexOf(splitter);
        return position<0 ? null : string.substring(position+splitter.length());
    }

    public static boolean doesUseQuotation(int type) {
        boolean flag = false;
        switch(type) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                flag = true;
                break;

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                flag = true;
                break;

            default:
                break;
        }
        return flag;
    }

    public static String normalizeKeyword(final String keyword) {
        if ((null==keyword) || (keyword.length()==0)) {
            return keyword;
        }
        String value = keyword;
        int i1 = keyword.indexOf('(');
        if (i1 > 0) {
            int i2 = keyword.indexOf(')', i1);
            if (i2 > 0) {
                if (keyword.substring(i1, i2+1).contains("site")) {
                    String tmp = value.substring(0, i1);
                    if ((i2+1) < value.length()) {
                        tmp = tmp + value.substring(i2+1);
                    }
                    value = tmp;
                }
            }
        }

        try {
            value = URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Can not decode "+keyword, e);
        } catch (Exception e) {
        }
        value = WHITE_SPACE_PATTERN.matcher(value).replaceAll(" ");
        return value.trim();
    }

    public static String getDigit(String value, String defaultValue) {
        if (null == value) {
            return defaultValue;
        }
        value = value.trim();
        if ("".equals(value)) {
            return defaultValue;
        }
        int i = 0;
        for( ; i<value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                break;
            }
        }

        if (0 == i) {
            return defaultValue;
        } else if (i < value.length()) {
            return value.substring(0, i);
        }
        return value;
    }

    public static String decodeUrl(String url) {
        if (null==url || url.length()==0) {
            return "";
        }
        try {
            return URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Can not decode "+url, e);
        }
        return "";
    }

    private final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    private final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");
    private final static Pattern WHITE_SPACE_PATTERN = Pattern.compile("\\s{2,}");

    private final static Log log = LogFactory.getLog(Helper.class);
}
