package com.awesome.flink.internal.event;

public enum EventType {
    DML("DML"),

    DDL("DDL");

    EventType(String event) {
    }
}
