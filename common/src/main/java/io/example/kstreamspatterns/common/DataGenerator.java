package io.example.kstreamspatterns.common;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

public class DataGenerator {
  public static void main(String[] args) throws Exception {
    String topic = args.length > 0 ? args[0] : "input";
    Properties props = new Properties();
    props.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
    props.put("key.serializer", Serdes.String().serializer().getClass().getName());
    props.put("value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < 10; i++) {
        String key = UUID.randomUUID().toString();
        RecordMetadata meta =
            producer
                .send(new ProducerRecord<>(topic, key, "value-" + i))
                .get();
        System.out.printf("sent %s to %s@%d%n", key, meta.topic(), meta.offset());
        Thread.sleep(Duration.ofMillis(500).toMillis());
      }
    }
  }
}
