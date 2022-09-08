package com.awesome.flink.internal.event;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CdcEvent {
    EventType eventType;
    String db;
    String tableName;
    Long time;

    protected CdcEvent(String db, String tableName, Long time) {
        this.db = db;
        this.tableName = tableName;
        this.time = time;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getDb() {
        return db;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getTime() {
        return time;
    }

    public String getIdentifier() {
        return String.join(".", getDb(), getTableName());
    }

    public String getTargetTableName(String instance) {
        return String.join("_", instance, getDb(), getTableName());
    }

    public String getExecutableSql(String instance) {
        return "";
    }

    public static Boolean isDdlEvent(SourceRecord record) {
        return record.topic().equals("mysql_binlog_source");
    }

    public static class Builder {
        protected final SourceRecord record;
        protected String db;
        protected String tableName;
        protected Long time;

        public Builder(SourceRecord record) {
            this.record = record;
            extractDb();
            extractTable();
            extractTime();
        }

        private void extractDb() {
            db = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getString("db");
        }

        private void extractTable() {
            tableName = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getString("table");
        }

        private void extractTime() {
            time = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getInt64(Envelope.FieldName.TIMESTAMP);
        }

        public CdcEvent build() {
            return new CdcEvent(db, tableName, time);
        }
    }
}
