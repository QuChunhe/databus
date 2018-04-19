package databus.listener.mysql2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created by Qu Chunhe on 2018-04-19.
 */
public class WriteRowsProcessor {

    protected String toString(Serializable value,  int type) {
        if ((Types.TIMESTAMP==type) && (value instanceof Date) && !(value instanceof Timestamp)) {
            return DATE_FORMAT.format(((Date) value).toInstant());
        }
        return  value.toString();
    }

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withZone(ZoneId.of("GMT"));
}
