package com.leqee.etl.internal.event;

import com.leqee.etl.util.JsonConvertor;
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
    public String toString() {
        return "DDLEvent{" +
                "eventType=" + eventType +
                ", db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                ", time=" + time +
                ", ddl='" + ddl + '\'' +
                '}';
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
