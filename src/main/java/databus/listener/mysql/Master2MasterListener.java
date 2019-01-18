package databus.listener.mysql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2016-10-27.
 */
public class Master2MasterListener extends MysqlListener {

    public Master2MasterListener() {
        super();
    }

    @Override
    public void onEvent(Event event) {
        if (null == duplicateRowFilter.process(event)) {
            log.info("REJECT: " + event.toString());
        } else {
            super.onEvent(event);
        }
    }

    public void setDuplicateRowFilter(DuplicateRowFilter duplicateRowFilter) {
        this.duplicateRowFilter = duplicateRowFilter;
        duplicateRowFilter.load();
        for(String fullName : permittedTableSet) {
            String table = fullName.split("\\.")[1];
            duplicateRowFilter.addFilteredTable(table);
        }
    }

    private static Log log = LogFactory.getLog(Master2MasterListener.class);

    private DuplicateRowFilter duplicateRowFilter;
}
