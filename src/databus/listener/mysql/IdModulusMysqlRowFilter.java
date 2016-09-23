package databus.listener.mysql;

import java.math.BigInteger;
import java.sql.Types;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.EventFilter;
import databus.event.MysqlEvent;
import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.Column;

public class IdModulusMysqlRowFilter implements EventFilter {

    @Override
    public boolean reject(Event event) {
        if (event instanceof AbstractMysqlWriteRow) {
            return reject0((AbstractMysqlWriteRow) event);
        }
        return false;
    }

    @Override
    public void initialize(Properties properties) {
        String idValue = properties.getProperty("id");
        if (null == idValue) {
            log.error("id is null!");
            System.exit(1);
        }
        String[] idParts = idValue.split("/");
        if (idParts.length != 2) {
            log.error("id format is illegal : "+idValue);
            System.exit(1);
        }
        remainder = Long.parseUnsignedLong(idParts[0]);
        divisor = Long.parseUnsignedLong(idParts[1]);
        if (remainder >= divisor) {
            log.error("remainder is greater than divisor : "+idValue);
            System.exit(1);
        }

        String ignoreTableValue = properties.getProperty("ignoredTables");
        if (null == ignoreTableValue) {
            return;
        }
        String[] ignoreTableParts = ignoreTableValue.split(",");
        for(int i=0; i< ignoreTableParts.length; i++) {
            String table = ignoreTableParts[i].trim();
            if (table.length() > 0) {
                ignoreTables.add(table);
            }
        }
    }   
    
    @Override
    public String toString() {
        return getClass().getName()+ " [divisor=" + divisor + ", remainder=" +
               remainder + ", ignoreTables=" + ignoreTables.toString() + "]";
    }

    private boolean reject0(AbstractMysqlWriteRow event) {
        if (!MysqlEvent.Type.INSERT.toString().equals(event.type())) {
            return false;
        }
        if (ignoreTables.contains(event.database()+"."+event.table())) {
            return false;
        }
        for(Column column : event.primaryKeys()) {
            long r = 0;
            String value = column.value();
            switch(column.type()) {
            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER :
                r = Long.parseLong(value) % divisor;
                break;
            case Types.BIGINT :
                BigInteger id = new BigInteger(value);
                r = id.remainder(BigInteger.valueOf(divisor)).longValue();
                break;
            default:
                continue;    
            }
            if (r != remainder) {
                return true;
            }
        }
        return false;
    }
    
    private static Log log = LogFactory.getLog(IdModulusMysqlRowFilter.class);
    
    private long divisor;
    private long remainder;
    private Set<String> ignoreTables = new HashSet<String>();

}
