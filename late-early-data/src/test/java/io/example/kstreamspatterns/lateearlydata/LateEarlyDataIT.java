package io.example.kstreamspatterns.lateearlydata;

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

public class LateEarlyDataIT extends KafkaIntegrationTest {
  @Test
  void emitsEarlyAndFinalResultsAndDropsLateRecords() {
    System.setProperty("input.topic", "input-late-early-it");
    System.setProperty("output.topic", "output-late-early-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "late-early-data-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put("key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put("value.serializer", Serdes.Long().serializer().getClass().getName());
    try (KafkaProducer<String, Long> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-late-early-it", null, 0L, "k", 1L));
      producer.send(new ProducerRecord<>("input-late-early-it", null, 2000L, "advance", 0L));
      producer.send(new ProducerRecord<>("input-late-early-it", null, 3000L, "k", 1L));
      producer.send(new ProducerRecord<>("input-late-early-it", null, 11000L, "advance2", 0L));
      producer.send(new ProducerRecord<>("input-late-early-it", null, 12000L, "k", 1L));
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
      consumer.subscribe(List.of("output-late-early-it"));
      ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(10));
      List<Long> counts =
          java.util.stream.StreamSupport
              .stream(records.records("output-late-early-it").spliterator(), false)
              .filter(r -> r.key().startsWith("k@"))
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());
      assertThat(counts).containsExactly(1L, 2L);
    }

    streams.close();
  }
}
