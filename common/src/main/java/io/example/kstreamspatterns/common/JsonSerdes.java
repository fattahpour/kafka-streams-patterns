package io.example.kstreamspatterns.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerdes {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonSerdes() {}

  public static <T> Serde<T> serde(Class<T> clazz) {
    Serializer<T> serializer =
        (topic, data) -> {
          try {
            return MAPPER.writeValueAsBytes(data);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
    Deserializer<T> deserializer =
        (topic, data) -> {
          try {
            return MAPPER.readValue(data, clazz);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
    return Serdes.serdeFrom(serializer, deserializer);
  }
}
