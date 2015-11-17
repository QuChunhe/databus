package databus.event.mysql;

import java.sql.Types;

public class Value {    
    public Value(String value, int type) {
        this.value = value;
        this.type = type;
    }

    public String value() {
        return value;
    }
    
    public int type() {
        return type;
    }
    
    public boolean isString() {
        switch(type) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
            return true;
        default:
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }    

    @Override
    public boolean equals(Object other) {
        if (other instanceof Value) {
            Value o = (Value) other;
            if ((type==o.type) && value.equals(o.value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return value;
    }

    private String value;
    private int type;
}
