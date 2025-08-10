package com.fattahpour.kstreamspatterns.exactlyonceoutbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class ExactlyOnceOutboxIT extends KafkaIntegrationTest {

  @Test
  void endToEnd() {
    System.setProperty("input.topic", "orders-in");
    System.setProperty("processed.topic", "orders-proc");
    System.setProperty("outbox.topic", "orders-out");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "eoo-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("orders-in", "k", "item"));
      producer.flush();
    }

    Properties consProps = new Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
      consumer.subscribe(Collections.singleton("orders-proc"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      assertThat(
              java.util.stream.StreamSupport
                  .stream(records.records("orders-proc").spliterator(), false)
                  .map(ConsumerRecord::value))
          .containsExactly("ITEM");
    }
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
      consumer.subscribe(Collections.singleton("orders-out"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      assertThat(
              java.util.stream.StreamSupport
                  .stream(records.records("orders-out").spliterator(), false)
                  .map(ConsumerRecord::value))
          .containsExactly("ITEM");
    }

    streams.close();
  }
}
