package databus.listener.mysql;

import databus.core.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by Qu Chunhe on 10/27/16.
 */
public class Master2MasterListener extends MysqlListener {

    public Master2MasterListener() {
        super();
    }

    @Override
    public void onEvent(Event event) {
        if (filter.doesReject(event)) {
            log.info("REJECT: " + event.toString());
        } else {
            super.onEvent(event);
        }
    }

    public void setDuplicateRowFilter(DuplicateRowFilter filter) {
        this.filter = filter;
        filter.load();
        for(String fullName : permittedTableSet) {
            String table = fullName.split("\\.")[1];
            filter.addFilteredTable(table);
        }
    }

    private static Log log = LogFactory.getLog(Master2MasterListener.class);

    private DuplicateRowFilter filter;
}
