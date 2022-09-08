package com.awesome.flink.internal.event;

import com.awesome.flink.internal.dialect.MySqlDialect;
import com.awesome.flink.util.CdcConfiguration;
import com.awesome.flink.util.JsonConvertor;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class DDLEvent extends CdcEvent {

    private final String ddl;

    private DDLEvent(String ddl, String db, String tableName, Long time) {
        super(db, tableName, time);
        this.ddl = ddl;
        this.eventType = EventType.DDL;
    }

    public String getDdl() {
        return ddl;
    }

    @Override
    public String getExecutableSql(String instance) {
        String ddl = getDdl();
        String targetTableName = getTargetTableName(instance);
        switch (SpecialDDLKeyWord.forCode(ddl)) {
            case TRUNCATE:
                ddl = MySqlDialect.handleTruncate(targetTableName);
                break;
            case DROP_TABLE:
                ddl = MySqlDialect.handleDropTable(targetTableName);
                break;
            case OTHER:
            default:
                ddl = getDdl()
                        .replaceAll(getDb(), CdcConfiguration.TARGET_INSTANCE_SCHEMA)
                        .replaceAll(getTableName(), targetTableName);
        }
        return ddl;
    }

    @Override
    public String toString() {
        return "DDLEvent{" +
                "eventType=" + eventType +
                ", db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                ", time=" + time +
                ", ddl='" + ddl + '\'' +
                '}';
    }

    private enum SpecialDDLKeyWord {
        TRUNCATE("truncate"),
        DROP_TABLE("drop table"),
        OTHER("other");

        private final String keyWord;

        SpecialDDLKeyWord(String keyWord) {
            this.keyWord = keyWord;
        }

        private static SpecialDDLKeyWord forCode(String ddl) {
            for (SpecialDDLKeyWord keyWord : SpecialDDLKeyWord.values()) {
                if (ddl.toLowerCase().contains(keyWord.getKeyWord())) {
                    return keyWord;
                }
            }
            return OTHER;
        }

        private String getKeyWord() {
            return keyWord;
        }
    }

    public static class Builder extends CdcEvent.Builder {
        private String ddl;

        public Builder(SourceRecord record) {
            super(record);
        }

        public DDLEvent.Builder extractDdl() throws Exception {
            String ddlJson = ((Struct) record.value()).getString("historyRecord");
            ddl = JsonConvertor.extractDdl(ddlJson);
            return this;
        }

        @Override
        public DDLEvent build() {
            return new DDLEvent(ddl, db, tableName, time);
        }
    }
}
