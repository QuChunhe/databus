package databus.listener.mysql2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import databus.event.mysql.ColumnAttribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2018-03-16.
 */
public class SchemaCache {

    public SchemaCache(MysqlDataSource ds, Set<String> replicatedTableSet) {
        loadSchema(ds, replicatedTableSet);
    }

    public String[] getColumns(String fullTableName) {
        return columnsMap.get(fullTableName);
    }

    public ColumnAttribute[] getTypes(String fullTableName) {
        return attributesMap.get(fullTableName);
    }

    public Set<String> getPrimaryKeys(String fullTableName) {
        return primaryKeysMap.get(fullTableName);
    }

    public boolean hasChangedScheme(String fullTableName, int columnsLength) {
        String[] columns = getColumns(fullTableName);
        return (null==columns) || (columns.length!=columnsLength);
    }

    private void loadSchema(MysqlDataSource ds, Set<String> replicatedTableSet) {
        HashMap<String, Set<String>> tablesMap = new HashMap<>();
        for(String fullTableName : replicatedTableSet) {
            String[] r = fullTableName.split("\\.");
            if (r.length != 2) {
                log.error(fullTableName+" can not be split normally");
                System.exit(1);
            }
            String databaseName = r[0].trim().toLowerCase();
            String tableName = r[1].trim().toLowerCase();
            Set<String> tables = tablesMap.get(databaseName);
            if (null == tables) {
                tables = new HashSet<>();
                tablesMap.put(databaseName, tables);
            }
            tables.add(tableName);
        }

        columnsMap = new HashMap<>();
        attributesMap = new HashMap<>();
        primaryKeysMap = new HashMap<>();

        for(String databaseName : tablesMap.keySet()) {
            ds.setDatabaseName(databaseName);
            String fullName = "";
            try (Connection conn = ds.getConnection()){
                DatabaseMetaData metaData = conn.getMetaData();
                Set<String> tables = tablesMap.get(databaseName);
                for(String tableName : tables) {
                    LinkedList<String> columns = new LinkedList<>();
                    LinkedList<ColumnAttribute> attribute = new LinkedList<>();
                    fullName = databaseName + "." + tableName;
                    try (ResultSet resultSet1 = metaData.getColumns(null, "%",
                                                                    tableName, "%")) {
                        while (resultSet1.next()) {
                            columns.addLast(resultSet1.getString("COLUMN_NAME").toLowerCase());
                            int type = resultSet1.getInt("DATA_TYPE");
                            String typeName = resultSet1.getString("TYPE_NAME").toLowerCase();
                            attribute.addLast(new ColumnAttribute(type, typeName));
                        }
                    }
                    columnsMap.put(fullName, columns.toArray(new String[columns.size()]));
                    attributesMap.put(fullName,
                            attribute.toArray(new ColumnAttribute[attribute.size()]));

                    HashSet<String> keys = new HashSet<>();
                    try (ResultSet resultSet2 = metaData.getPrimaryKeys(null, null, tableName)) {
                        while(resultSet2.next()) {
                            keys.add(resultSet2.getString("COLUMN_NAME").toLowerCase());
                        }
                        primaryKeysMap.put(fullName, keys);
                    }
                }
            } catch (SQLException e) {
                log.error("Cannot load the schema of "+fullName, e);
                System.exit(1);
            }
        }
    }

    private final static Log log = LogFactory.getLog(SchemaCache.class);

    private Map<String, String[]> columnsMap;
    private Map<String, ColumnAttribute[]> attributesMap;
    private Map<String, Set<String>> primaryKeysMap;

}
