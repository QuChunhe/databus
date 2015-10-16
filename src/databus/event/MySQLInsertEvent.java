package databus.event;

import java.util.List;

import com.google.code.or.common.glossary.Row;


public class MySQLInsertEvent extends MySQLAbstractEvent<Row>{    
    protected List<Row> rows;

    @Override
    public List<Row> getRows() {
        return rows;
    }

}
