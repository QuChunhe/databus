package databus.listener.mysql;

import java.math.BigInteger;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.BlobColumn;
import com.google.code.or.common.glossary.column.Datetime2Column;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.TinyColumn;

import databus.event.mysql.AbstractMysqlWriteRow;

public abstract class MysqlWriteEventFactory {
    
    abstract public  boolean hasMore();
    
    abstract public AbstractMysqlWriteRow next();
    
    public String toString(Column column, ColumnAttribute attribute) {
        if ((null==column) || (column.getValue()==null)) {
            return null;
        }
        if (column instanceof BlobColumn) {
            return new String(((BlobColumn)column).getValue());
        }
        String value = column.toString();
        int type = attribute.type();
        if (attribute.isUnsigned() && isInt(type) && isNegative(value)) {
            BigInteger i = new BigInteger(column.toString());
            if (column instanceof LongLongColumn) {
                i = i.and(BIGINT_MASK);
            } else if (column instanceof LongColumn) {
                i = i.and(INTEGER_MASK); 
            } else if (column instanceof Int24Column) {
                i = i.and(MEDIUMINT_MASK);
            } else if (column instanceof ShortColumn) {
                i = i.and(SMALLINT_MASK);
            } else if (column instanceof TinyColumn) {
                i = i.and(TINYINT_MASK); 
            }
              
            return i.toString();
        }
        if ((column instanceof DatetimeColumn) || (column instanceof Datetime2Column)) {
            Date date = (Date) column.getValue();
            return DATE_FORMAT.format(date);
        }
        return column.toString();
    }
    
    private boolean isNegative(String value) {
        return value.trim().startsWith("-");
    }
    
    private boolean isInt(int type) {
        boolean isInt = false;
        switch (type) {
        case Types.BIGINT:
        case Types.INTEGER:
       
        case Types.SMALLINT:
        case Types.TINYINT:
            isInt = true;
            break;
        default:
            break;  
        }
        return isInt;
    }
    
    private static final BigInteger ONE = BigInteger.valueOf(1);
    private static final BigInteger BIGINT_MASK = ONE.shiftLeft(8*8).subtract(ONE);
    private static final BigInteger INTEGER_MASK = ONE.shiftLeft(4*8).subtract(ONE);
    private static final BigInteger MEDIUMINT_MASK = ONE.shiftLeft(3*8).subtract(ONE);
    private static final BigInteger SMALLINT_MASK = ONE.shiftLeft(2*8).subtract(ONE);
    private static final BigInteger TINYINT_MASK = ONE.shiftLeft(1*8).subtract(ONE);
    
    private static final SimpleDateFormat DATE_FORMAT = 
                                              new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");

}
