package com.leqee.etl.internal.dialect;

import java.util.List;
import java.util.stream.Collectors;

public class MySqlDialect {

    public static String getUpsertStatement(
            String tableName, List<String> fieldNames, List<String> fieldValues) {
        String updateClause = fieldNames
                        .stream()
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return getInsertIntoStatement(tableName, fieldNames, fieldValues)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause;
    }

    private static String getInsertIntoStatement(String tableName, List<String> fieldNames, List<String> fieldValues) {
        String columns = fieldNames
                        .stream()
                        .map(MySqlDialect::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String literal = String.join(", ", fieldValues);
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + literal
                + ")";
    }

    public static String quoteIdentifier(String anyKeyword) {
        return String.join("", "`", anyKeyword, "`");
    }

    public static String isDelColumn(String targetTableName) {
        final String isDel = "ALTER TABLE %s ADD is_del TINYINT NOT NULL DEFAULT 0";
        return String.format(isDel, quoteIdentifier(targetTableName));
    }

    public static String etlTimeColumn(String targetTableName) {
        final String etlTime = "ALTER TABLE %s ADD etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        return String.format(etlTime, quoteIdentifier(targetTableName));
    }
}
