package io.example.kstreamspatterns.rekeyrepartition;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
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

public class RekeyRepartitionIT extends KafkaIntegrationTest {
  @Test
  void endToEndRepartition() {
    System.setProperty("input.topic", "input-rekey-it");
    System.setProperty("output.topic", "output-rekey-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rekey-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put("key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put("value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-rekey-it", null, "u1:a"));
      producer.send(new ProducerRecord<>("input-rekey-it", null, "u2:b"));
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
      consumer.subscribe(Collections.singleton("output-rekey-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      assertThat(
              java.util.stream.StreamSupport
                  .stream(records.records("output-rekey-it").spliterator(), false)
                  .map(ConsumerRecord::key))
          .containsExactlyInAnyOrder("u1", "u2");
    }

    streams.close();
  }
}
