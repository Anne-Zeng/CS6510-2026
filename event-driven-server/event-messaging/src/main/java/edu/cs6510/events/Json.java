package edu.cs6510.events;

import tools.jackson.databind.json.JsonMapper;

/** One JSON representation for messages; Jackson 3 supports Java time and records. */
public final class Json {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private Json() {}
    public static String write(Object value) { return MAPPER.writeValueAsString(value); }
    public static <T> T read(String value, Class<T> type) { return MAPPER.readValue(value, type); }
}
