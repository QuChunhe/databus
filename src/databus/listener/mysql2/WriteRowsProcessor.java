package databus.listener.mysql2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created by Qu Chunhe on 2018-04-19.
 */
public class WriteRowsProcessor {

    protected String toString(Serializable value,  int type) {
        if (Types.TIMESTAMP == type) {
            if (value instanceof Timestamp) {
                return SIMPLE_DATE_FORMAT.format((Date) value);
            } else if (value instanceof Date){
                return DATE_TIME_FORMATTER.format(((Date) value).toInstant());
            }
        }
        return  value.toString();
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withZone(ZoneId.of("GMT"));
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
}
