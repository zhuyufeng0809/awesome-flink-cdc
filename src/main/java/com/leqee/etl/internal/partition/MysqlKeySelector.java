package com.leqee.etl.internal.partition;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import com.google.common.collect.Lists;
import com.leqee.etl.util.JsonConvertor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MysqlKeySelector extends RichKeySelector<String, Optional<String>> implements CheckpointedFunction {
    private String instance;
    private String url;
    private String username;
    private String pwd;
    private JsonConvertor convertor;

    private Map<String, TableInfo> tables;
    private transient ListState<TableInfo> tablesState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        this.convertor = JsonConvertor.build();
        this.tables = new HashMap<>();
        initializeConfig();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * @param value is the original JSON string
     * @return the reason why the return value is optional is that some tables may not have keys
     */
    @Override
    public Optional<String> getKey(String value) throws Exception {
        return Optional.ofNullable(extractKeys(value));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.tablesState.clear();
        this.tablesState.addAll(Lists.newArrayList(this.tables.values()));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        initializeConfig();
        ListStateDescriptor<TableInfo> tableStateDescriptor =
                new ListStateDescriptor<>("table-state-descriptor", TableInfo.class);
        this.tablesState = context.getOperatorStateStore().getListState(tableStateDescriptor);

        // restore logic

    }

    /**
     * The purpose of using distributed cache is to dynamically modify the configuration.
     * After saving with savepoint, modify the configuration file, and then recover from savepoint.
     */
    private void initializeConfig() throws Exception {
        ParameterTool distributedCacheFile = ParameterTool.fromPropertiesFile(getRuntimeContext().getDistributedCache().getFile("XXX"));
        this.instance = distributedCacheFile.get("");
        this.url = distributedCacheFile.get("");
        this.username = distributedCacheFile.get("");
        this.pwd = distributedCacheFile.get("");
    }

    private String extractKeys(String json) throws Exception {
        String fullPathName = getFullPathName(json);

        if (!contain(fullPathName)) {
            Connection conn = getDbConnection();
            String schema = getSchema(json);
            String tableName = getTableName(json);

            TableInfo table = new TableInfo(getInstance(), schema, tableName);
            table.setKeys(getTableKey(conn.getMetaData(), schema, tableName));

            addTable(fullPathName, table);
            conn.close();
        }

        return getKeyColumns(fullPathName)
                .stream()
                .map(s -> extractValue(json, s))
                .map(Optional::get)
                .collect(Collectors.joining());
    }

    private Connection getDbConnection() throws Exception {
        return DriverManager.getConnection(url, username, pwd);
    }

    private String getFullPathName(String json) throws Exception {
        return String.join(".", getInstance(), getSchema(json), getTableName(json));
    }

    private String getInstance() {
        return this.instance;
    }

    private String getSchema(String json) throws Exception {
        return JsonConvertor.extractSchema(json);
    }

    private String getTableName(String json) throws Exception {
        return JsonConvertor.extractTableName(json);
    }

    private List<String> getKeyColumns(String fullPathName) {
        return this.tables.get(fullPathName).getKeys();
    }

    private Optional<String> extractValue(String json, String column) {
        return JsonConvertor.extractValue(json, column);
    }

    private boolean contain(String fullPathName) {
        return this.tables.containsKey(fullPathName);
    }

    private List<String> getTableKey(DatabaseMetaData metaData, String schema, String table) throws SQLException {
        List<String> uniqueKeys = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        ResultSet idxUnique = metaData.getIndexInfo(schema, null, table, true, false);
        ResultSet idxPrimary = metaData.getPrimaryKeys(schema, null, table);

        while (idxUnique.next()) {
            uniqueKeys.add(idxUnique.getString("COLUMN_NAME"));
        }

        while (idxPrimary.next()) {
            primaryKeys.add(idxPrimary.getString("COLUMN_NAME"));
        }

        //prefer to use uniqueKey
        uniqueKeys.removeAll(primaryKeys);

        return uniqueKeys.size() == 0 ? primaryKeys : uniqueKeys;
    }

    private void addTable(String fullPathName, TableInfo table) {
        this.tables.put(fullPathName, table);
    }
}
