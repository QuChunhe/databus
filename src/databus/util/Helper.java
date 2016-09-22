package databus.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import io.netty.util.internal.ThreadLocalRandom;

public class Helper {
    
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
    
    public static String normalizeSocketAddress(String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            return null;
        }
        String normalizedAddress = null;
        try {
            int port = Integer.parseInt(parts[1]);
            if (port >= (1<<16)) {
                return null;
            }
            InetAddress inetAddress = InetAddress.getByName(parts[0]);
            normalizedAddress = inetAddress.getHostAddress() + ":" + port;
        } catch(Exception e) {                
        }
        return normalizedAddress;        
    }
    
    public static String toAlias(String name) {
        return name.replace('.', '_')
                   .replace('/', '-')
                   .replace(':', '-');
    }
    
    private static int rand() {
        return ThreadLocalRandom.current().nextInt(randomBound);
    }
    
    private static int randomBound = 1 << 22;
    private static int serviceIdMask = (1 << 10) - 1;
    
    protected final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    protected final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");
}
