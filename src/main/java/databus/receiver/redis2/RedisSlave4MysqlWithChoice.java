package databus.receiver.redis2;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Qu Chunhe on 2019-01-10.
 */
public class RedisSlave4MysqlWithChoice extends RedisSlave4Mysql {

    public RedisSlave4MysqlWithChoice() {
        super();
    }

    public void setReplicatedTables(Collection<String> replicatedTables) {
        this.replicatedTables = new HashSet<>(replicatedTables);
    }

    @Override
    protected Table getTable(String tableName) {
        if (replicatedTables.contains(tableName)) {
            return super.getTable(tableName);
        }
        return null;
    }

    private Set<String> replicatedTables;
}
