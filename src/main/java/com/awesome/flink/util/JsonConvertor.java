package com.awesome.flink.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

public class JsonConvertor {

    private final static ObjectMapper mapper = new ObjectMapper();

    private JsonConvertor() {
    }

    public static String extractSchema(String json) throws Exception {
        return extractSource(json).get("db").toString();
    }

    public static String extractTableName(String json) throws Exception {
        return extractSource(json).get("table").toString();
    }

    public static Optional<String> extractValue(String json, String column) {
        try {
            return Optional.ofNullable(extractAfter(json).get(column).toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }

    public static String extractDdl(String json) throws Exception {
        return getJsonTree(json).get("ddl").toString();
    }

    private static JsonNode extractAfter(String json) throws Exception {
        return getJsonTree(json).get("after");
    }

    private static JsonNode extractSource(String json) throws Exception {
        return getJsonTree(json).get("source");
    }

    private static JsonNode getJsonTree(String json) throws Exception {
        return mapper.readTree(json);
    }
}
