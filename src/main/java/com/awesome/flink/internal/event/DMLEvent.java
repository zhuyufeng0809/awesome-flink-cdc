package com.awesome.flink.internal.event;

import com.awesome.flink.internal.dialect.JdbcValueFormatter;
import com.awesome.flink.internal.dialect.MySqlDialect;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DMLEvent extends CdcEvent {

    private final List<Column> rowData;
    private final String op;

    private DMLEvent(List<Column> rowData, String db, String tableName, String op, Long time) {
        super(db, tableName, time);
        this.rowData = rowData;
        this.op = op;
        this.eventType = EventType.DML;
    }

    public List<Column> getRowData() {
        return rowData;
    }

    public List<String> getColumns() {
        return getRowData()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    public List<String> getValues() {
        return getRowData().stream()
                .map(Column::getValue)
                .map(JdbcValueFormatter::format)
                .collect(Collectors.toList());
    }

    @Override
    public String getExecutableSql(String instance) {
        return MySqlDialect.getUpsertStatement(getTargetTableName(instance),
                getColumns(),
                getValues());
    }

    public static Boolean isDelOperation(String op) {
        return op.equals("d");
    }

    @Override
    public String toString() {
        return "DMLEvent{" +
                "eventType=" + eventType +
                ", rowData=" + rowData +
                ", db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                ", op='" + op + '\'' +
                ", time=" + time +
                '}';
    }

    public static class Builder extends CdcEvent.Builder {
        private List<Column> data;
        private String op;

        public Builder(SourceRecord record) {
            super(record);
            this.data = new ArrayList<>();
        }

        public Builder extractRow() {
            Struct value = ((Struct) record.value());
            String op = value.getString(Envelope.FieldName.OPERATION);

            Struct beforeOrAfter;

            if (isDelOperation(op)) {
                beforeOrAfter = value.getStruct(Envelope.FieldName.BEFORE);
            } else {
                beforeOrAfter = value.getStruct(Envelope.FieldName.AFTER);
            }

            data = beforeOrAfter
                    .schema()
                    .fields()
                    .stream()
                    .map(field -> {
                        String name = field.name();
                        return new Column(name, beforeOrAfter.get(name), field.schema().name());
                    })
                    .collect(Collectors.toList());

            data.add(Column.auxiliaryIsDel(op));

            return this;
        }

        public Builder extractOp() {
            op = ((Struct) record.value()).getString(Envelope.FieldName.OPERATION);
            return this;
        }

        @Override
        public DMLEvent build() {
            return new DMLEvent(data, db, tableName, op, time);
        }
    }
}
