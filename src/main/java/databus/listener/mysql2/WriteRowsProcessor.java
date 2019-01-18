package databus.listener.mysql2;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.mysql.ColumnAttribute;

/**
 * Created by Qu Chunhe on 2018-04-19.
 */
public class WriteRowsProcessor {

    protected String toString(Serializable value, ColumnAttribute attribute) {
        if (null == value) {
            return null;
        }
        int type = attribute.type();
        if (Types.TIMESTAMP == type) {
            if (value instanceof Timestamp) {
                return SIMPLE_DATE_FORMAT.format((Date) value);
            } else if (value instanceof Date){
                return DATE_TIME_FORMATTER.format(((Date) value).toInstant());
            }
        }
        if (attribute.isUnsigned() && isNegative(value.toString())) {
            BigInteger i = new BigInteger(value.toString());
            switch (type) {
                case Types.BIGINT:
                    i = i.and(BIGINT_MASK);
                    break;
                case Types.INTEGER:
                    i = i.and(INTEGER_MASK);
                    break;
                default:
                    log.error("Can not convert "+type+" "+value.toString());
                    break;
            }
            return i.toString();
        }

        return  value.toString();
    }

    private boolean isNegative(String value) {
        return value.trim().startsWith("-");
    }

    private final static Log log = LogFactory.getLog(WriteRowsProcessor.class);

    private static final BigInteger ONE = BigInteger.valueOf(1);
    private static final BigInteger BIGINT_MASK = ONE.shiftLeft(8*8).subtract(ONE);
    private static final BigInteger INTEGER_MASK = ONE.shiftLeft(4*8).subtract(ONE);

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
                    DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withZone(ZoneId.of("GMT"));
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT =
                                              new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}
