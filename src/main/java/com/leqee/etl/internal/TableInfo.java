package com.leqee.etl.internal;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

/**
 * Note that neither hashcode nor equal methods
 * involve the keys field,
 * because the keys field may change
 */
public class TableInfo {
    private final String instance;
    private final String schema;
    private final String name;
    private final List<String> keys;

    public TableInfo(String instance, String schema, String name) {
        this.instance = instance;
        this.schema = schema;
        this.name = name;
        this.keys = new ArrayList<>();
    }

    public String getFullPathName() {
        return String.join(".", instance, schema, name);
    }

    public List<String> getKeys() {
        return this.keys;
    }

    public void setKeys(List<String> keys) {
        this.keys.addAll(keys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo tableInfo = (TableInfo) o;
        return Objects.equal(instance, tableInfo.instance)
                && Objects.equal(schema, tableInfo.schema)
                && Objects.equal(name, tableInfo.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(instance, schema, name);
    }

    @Override
    public String toString() {
        return getFullPathName() + ":" + String.join(",", keys);
    }
}
