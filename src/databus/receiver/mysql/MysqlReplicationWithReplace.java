package databus.receiver.mysql;

public class MysqlReplicationWithReplace extends MysqlReplication {
    public MysqlReplicationWithReplace() {
        super();
    }

    @Override
    protected String getPrefixInsertSql() {
        return "REPLACE INTO ";
    }
}
