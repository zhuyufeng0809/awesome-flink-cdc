package com.awesome.flink.internal.event;

import com.google.common.base.Objects;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class Column {
    private final String name;
    private final Object value;
    private final String temporalSemanticType;

    public Column(String name, Object value, String temporalSemanticType) {
        this.name = name;
        this.value = value;
        this.temporalSemanticType = temporalSemanticType;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        // there https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-temporal-types
        switch (TemporalSemanticType.forCode(getTemporalSemanticType())) {
            case Date:
                return parseToDate(value);
            case MicroTime:
                return parseToTime(value);
            case Timestamp:
                return parseFromMilliSeconds(value);
            case MicroTimestamp:
                return parseFromMicroseconds(value);
            case Other:
            default:
                return value;
        }
    }

    private LocalDate parseToDate(Object value) {
        if (value == null) {
            return null;
        } else {
            return LocalDate.parse("1970-01-01").plusDays(Long.parseLong(value.toString()));
        }
    }

    private LocalTime parseToTime(Object value) {
        // cause LocalTime does not provide `plusMicro` method,
        // so need convert micro to nanos
        if (value == null) {
            return null;
        } else {
            return LocalTime.parse("00:00:00").plusNanos(Long.parseLong(value.toString()) * 1000);
        }
    }

    private LocalDateTime parseFromMilliSeconds(Object value) {
        // cause LocalTime does not provide `plusMicro` method,
        // so need convert micro to nanos
        if (value == null) {
            return null;
        } else {
            return LocalDateTime.parse("1970-01-01T00:00:00").plusNanos(Long.parseLong(value.toString()) * 1000 * 1000);
        }
    }

    private LocalDateTime parseFromMicroseconds(Object value) {
        // cause LocalTime does not provide `plusMicro` method,
        // so need convert micro to nanos
        if (value == null) {
            return null;
        } else {
            return LocalDateTime.parse("1970-01-01T00:00:00").plusNanos(Long.parseLong(value.toString()) * 1000);
        }
    }

    public String getTemporalSemanticType() {
        return temporalSemanticType;
    }

    public static Column auxiliaryIsDel(String op) {
        if (DMLEvent.isDelOperation(op)) {
            return new Column("is_del", 1, null);
        } else {
            return new Column("is_del", 0, null);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column row = (Column) o;
        return Objects.equal(name, row.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", temporalSemanticType='" + temporalSemanticType + '\'' +
                '}';
    }
}
