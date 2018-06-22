package databus.event.mysql;

import databus.util.Helper;

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
    
    public boolean doesUseQuotation() {
        return Helper.doesUseQuotation(type);
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }    

    @Override
    public boolean equals(Object other) {
        if (other instanceof Column) {
            Column o = (Column) other;
            return (type==o.type) && value.equals(o.value) && name.equals(o.name);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(64);
        builder.append("{")
               .append("\"name\": \"")
               .append(name)
               .append("\", ")
               .append("\"value\": ");
        if (doesUseQuotation()) {
            builder.append("\"")
                   .append(value)
                   .append("\"");
        } else {
            builder.append(value);
        }
        builder.append(", ")
               .append("\"type\": ")
               .append(type)
               .append("}");
        return builder.toString();
    }

    private final String name;
    private final String value;
    private final int type;
    
}
