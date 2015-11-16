package databus.receiver;

import java.sql.Types;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.MysqlWriteRows;
import databus.event.mysql.MysqlDeleteRows;
import databus.event.mysql.MysqlInsertRows;
import databus.event.mysql.MysqlUpdateRows;

public class MysqlReplication extends MysqlReceiver{    

    public MysqlReplication() {
        super();
    }

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertRows) {
            insert((MysqlInsertRows) event);
        } else if (event instanceof MysqlUpdateRows) {
            update((MysqlUpdateRows)event);
        } else if (event instanceof MysqlDeleteRows) {
            delete((MysqlDeleteRows)event);
        } else {
            log.error("Can't process "+event.toString());
        }
    }

    private void insert(MysqlInsertRows event) {
        if (event.rows().size() == 0) {
            log.error("rows size is zero :"+event.toString());
            return;
        }
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(event.table().toLowerCase());
        sqlBuilder.append(" ");
        sqlBuilder.append(toInsertValuesPhrase(event.columns(),
                                               EMPTY_INDEX_SET));        
        sqlBuilder.append(" VALUES ");
        Set<Integer> indexSet = indexSetOfStringType(event.columnTypes());
        for(List<String> row : event.rows()) {
            sqlBuilder.append(toInsertValuesPhrase(row,indexSet));
            sqlBuilder.append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length()-1);
        String sql = sqlBuilder.toString();
        int count = executeWrite(sql);
        
        int expectedCount = event.rows().size();
        if (expectedCount != count) {
            log.error("Only "+count + " rows " + "in expected " + 
                      expectedCount + " rows have been inserted: "+sql);
        }
    }
    
    private void update(MysqlUpdateRows event) {
        if (event.rows().size() == 0) {
            log.error("rows size is zero :"+event.toString());
            return;
        }
        
        Set<Integer> indexSet = indexSetOfStringType(event.columnTypes());
        List<String> columnNames = event.columns();
        LinkedList<String> batchSql = new LinkedList<String>();
        HashSet<String> primaryKeys = new HashSet<String>(event.primaryKeys());
        String tableName = event.table().toLowerCase();
        for(MysqlUpdateRows.Entity entity : event.rows()) {
            String phase = toUpdateSetPhrase(entity, columnNames, indexSet);
            if (phase.length() > 0) {
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("UPDATE ");
                sqlBuilder.append(tableName); 
                sqlBuilder.append(" Set ");
                sqlBuilder.append(phase);
                sqlBuilder.append(" WHERE ");
                sqlBuilder.append(toWherePhrase(entity.after(), columnNames,
                                                primaryKeys, indexSet));
                batchSql.addLast(sqlBuilder.toString());
            }           
        }
        
        checkResult(executeWrite(batchSql), event);
    }
    
    private void delete(MysqlDeleteRows event) {
        if (event.rows().size() == 0) {
            log.error("rows size is zero :"+event.toString());
            return;
        }
        
        Set<Integer> indexSet = indexSetOfStringType(event.columnTypes());
        String tableName = event.table().toLowerCase();
        List<String> columnNames = event.columns();
        HashSet<String> primaryKeys = new HashSet<String>(event.primaryKeys());
        LinkedList<String> batchSql = new LinkedList<String>();
        for(List<String> row : event.rows()) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            sqlBuilder.append(tableName);
            sqlBuilder.append(" WHERE ");
            sqlBuilder.append(toWherePhrase(row, columnNames, 
                                            primaryKeys, indexSet));
            batchSql.addLast(sqlBuilder.toString());
        }

        checkResult(executeWrite(batchSql), event);
    }
    
    private Set<Integer> indexSetOfStringType(List<Integer> types) {
        HashSet<Integer> indexSet = new HashSet<Integer>();
        ListIterator<Integer> it = types.listIterator();
        while(it.hasNext()) {
            Integer index = it.nextIndex();
            int t = it.next();
            if((Types.CHAR == t) ||(Types.VARCHAR == t) ||
               (Types.NCHAR == t) || (Types.NVARCHAR == t) ||
               (Types.LONGVARCHAR == t) ||(Types.LONGNVARCHAR == t)) {
                indexSet.add(index);
            }
        }
        return indexSet;
    }
    
    private String toInsertValuesPhrase(List<String> row, 
                                        Set<Integer> indexSet) {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        ListIterator<String> it = row.listIterator();
        while(it.hasNext()) {
            int index = it.nextIndex();
            String value = it.next();
            append(builder, value, indexSet.contains(index));
            if (it.hasNext()) {
               builder.append(','); 
            }            
        }
        builder.append(')');
        return builder.toString();
    }
    
    private String toUpdateSetPhrase(MysqlUpdateRows.Entity entity, 
                             List<String> columnNames, Set<Integer> indexSet) {
        StringBuilder builder = new StringBuilder();
        ListIterator<String> nameIt = columnNames.listIterator();
        ListIterator<String> beforeIt = entity.before().listIterator();
        ListIterator<String> afterIt = entity.after().listIterator();
        while(nameIt.hasNext()) {
            String before = beforeIt.next();
            String after = afterIt.next();
            int index = nameIt.nextIndex();
            String name = nameIt.next();
            if (equals(before, after)) {
                continue;
            } 
            builder.append(name);
            builder.append('=');
            append(builder, after, indexSet.contains(index));
            builder.append(',');           
        }
        builder.deleteCharAt(builder.length()-1);
        return builder.toString();
    }
    
    private String toWherePhrase(List<String> row, List<String> columnNames,
                              Set<String> primaryKeys, Set<Integer> indexSet) {
        StringBuilder builder = new StringBuilder();
        ListIterator<String> rowIt = row.listIterator();
        ListIterator<String> nameIt = columnNames.listIterator();
        while(nameIt.hasNext()) {
            int index = nameIt.nextIndex();
            String name = nameIt.next();
            String value = rowIt.next();            
            if (primaryKeys.contains(name)) {
                builder.append(name);
                builder.append('=');
                append(builder, value, indexSet.contains(index));
                builder.append(',');  
            }
        }
        builder.deleteCharAt(builder.length()-1);
        return builder.toString();        
    }
    
    private void append(StringBuilder builder, String value, boolean isString) {
        if (!isString) {
            builder.append(value);
        } else {
            if (null == value) {
                builder.append("NULL");
            } else {
                builder.append("'");
                builder.append(value.replace("'", "\\'"));
                builder.append("'");
            }
        }
    }
    
    private boolean equals(String one, String other) {
        if (null == one) {
            if (null == other) {
                return true;
            } else {
                return false;
            }
        } else {
            return one.equals(other);
        }
    }
    
    private void checkResult(int[] count, MysqlWriteRows<?> event) {
        int num = 0;        
        for(int c : count) {
            num += c;
        }
        int expectedCount = event.rows().size();
        if (num == expectedCount) {
            return;
        }
        log.error("Only " + num + " rows in expected " + expectedCount + 
                  " rows have been written : " + event.toString());
    }
    
    private static Log log = LogFactory.getLog(MysqlReplication.class);
    private Set<Integer> EMPTY_INDEX_SET = new HashSet<Integer>();
}
