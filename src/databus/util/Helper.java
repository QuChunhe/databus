package databus.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

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
    
    private final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    private final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");

    private final static Log log = LogFactory.getLog(Helper.class);
}
