package databus.event.mysql;

import java.util.Comparator;

public class ColumnComparator implements Comparator<Column> {

    @Override
    public int compare(Column c1, Column c2) {
        return c1.name().compareTo(c2.name());
    }        
}
