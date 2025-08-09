package io.example.kstreamspatterns.suppression;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class SuppressionIT extends KafkaIntegrationTest {
  @Test
  void emitsOnlyFinalResults() {
    System.setProperty("input.topic", "input-suppression-it");
    System.setProperty("output.topic", "output-suppression-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppression-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put(
        "key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put(
        "value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-suppression-it", null, 0L, "k1", "a"));
      producer.send(new ProducerRecord<>("input-suppression-it", null, 1000L, "k1", "b"));
      producer.send(
          new ProducerRecord<>(
              "input-suppression-it",
              null,
              Duration.ofMinutes(1).toMillis() + 1,
              "k2",
              "x"));
      producer.flush();
    }

    Properties consProps = new Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass());
    consProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.Long().deserializer().getClass());
    try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consProps)) {
      consumer.subscribe(List.of("output-suppression-it"));
      ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(10));
      List<Long> values =
          records.records("output-suppression-it").stream()
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());
      assertThat(values).containsExactly(2L);
    }

    streams.close();
  }
}
