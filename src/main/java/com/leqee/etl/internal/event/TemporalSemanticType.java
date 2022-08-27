package com.leqee.etl.internal.event;

public enum TemporalSemanticType {
    Date("io.debezium.time.Date"),

    MicroTime("io.debezium.time.MicroTime"),

    Timestamp("io.debezium.time.Timestamp"),

    MicroTimestamp("io.debezium.time.MicroTimestamp"),

    Other("other");

    private final String type;

    TemporalSemanticType(String type) {
        this.type = type;
    }

    public static TemporalSemanticType forCode(String code) {
        for (TemporalSemanticType type : TemporalSemanticType.values()) {
            if (type.getType().equalsIgnoreCase(code)) {
                return type;
            }
        }
        return Other;
    }

    public String getType() {
        return type;
    }
}
