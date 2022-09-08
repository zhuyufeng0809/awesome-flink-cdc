package com.awesome.flink.internal.dialect;

import com.awesome.flink.util.CdcConfiguration;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
        final String isDel = "ALTER TABLE %s.%s ADD is_del TINYINT NOT NULL DEFAULT 0";
        return String.format(isDel,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName));
    }

    public static String isDelIndex(String targetTableName) {
        final String index = "ALTER TABLE %s.%s ADD INDEX idx_is_del (is_del)";
        return String.format(index,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName));
    }

    public static String etlTimeColumn(String targetTableName) {
        final String etlTime = "ALTER TABLE %s.%s ADD etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        return String.format(etlTime,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName));
    }

    public static String etlTimeIndex(String targetTableName) {
        final String index = "ALTER TABLE %s.%s ADD INDEX idx_etl_time (etl_time)";
        return String.format(index,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName));
    }

    public static String handleTruncate(String targetTableName) {
        final String update = "UPDATE %s.%s SET is_del = 1";
        return String.format(update,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName));
    }

    public static String handleDropTable(String targetTableName) {
        final String rename = "ALTER TABLE %s.%s RENAME TO %s";
        String nowTimestamp = String.valueOf(LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8")));
        String newTargetTableName = String.join("_", targetTableName, "bak", nowTimestamp);
        return String.format(rename,
                quoteIdentifier(CdcConfiguration.TARGET_INSTANCE_SCHEMA),
                quoteIdentifier(targetTableName),
                quoteIdentifier(newTargetTableName));
    }
}
