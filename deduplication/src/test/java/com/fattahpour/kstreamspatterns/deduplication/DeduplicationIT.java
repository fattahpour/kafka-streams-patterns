package com.fattahpour.kstreamspatterns.deduplication;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.KafkaIntegrationTest;
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

public class DeduplicationIT extends KafkaIntegrationTest {
  @Test
  void endToEndDeduplication() {
    System.setProperty("input.topic", "input-dedup-it");
    System.setProperty("output.topic", "output-dedup-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-it");
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
      producer.send(new ProducerRecord<>("input-dedup-it", null, 0L, "k1", "a"));
      producer.send(new ProducerRecord<>("input-dedup-it", null, 1000L, "k1", "b"));
      producer.send(
          new ProducerRecord<>(
              "input-dedup-it", null, Duration.ofMinutes(11).toMillis(), "k1", "c"));
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
        Serdes.String().deserializer().getClass());
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
      consumer.subscribe(List.of("output-dedup-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      List<String> values =
          java.util.stream.StreamSupport
              .stream(records.records("output-dedup-it").spliterator(), false)
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());
      assertThat(values).containsExactly("a", "c");
    }

    streams.close();
  }
}
