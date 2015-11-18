package databus.event.mysql;

import java.sql.Types;

public class Column {    
    public Column(String name, String value, int type) {
        this.name = name;
        this.value = value;
        this.type = type;
    }
    
    public String name() {
        return name;
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
        if (other instanceof Column) {
            Column o = (Column) other;
            if ((type==o.type) && 
                value.equals(o.value) && 
                name.equals(o.name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "{name: "+name+", value:"+value+", type: "+type+"}";
    }

    private String name;
    private String value;
    private int type;
}
