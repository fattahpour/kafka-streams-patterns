package io.example.kstreamspatterns.aggwindowhopping;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class AggWindowHoppingIT extends KafkaIntegrationTest {
  @Test
  void endToEndAggregation() {
    System.setProperty("input.topic", "hopping-input-it");
    System.setProperty("output.topic", "hopping-output-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-window-hopping-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put("key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put("value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("hopping-input-it", null, 0L, "k1", "v1"));
      producer.send(new ProducerRecord<>("hopping-input-it", null, 30000L, "k1", "v2"));
      producer.send(new ProducerRecord<>("hopping-input-it", null, 60000L, "k1", "v3"));
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
      consumer.subscribe(Collections.singletonList("hopping-output-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      List<KeyValue<String, String>> values = new ArrayList<>();
      records.forEach(r -> values.add(KeyValue.pair(r.key(), r.value())));
      assertThat(values)
          .containsExactly(
              KeyValue.pair("k1@0", "1"),
              KeyValue.pair("k1@0", "2"),
              KeyValue.pair("k1@30000", "1"),
              KeyValue.pair("k1@60000", "1"),
              KeyValue.pair("k1@30000", "2"));
    }

    streams.close();
  }
}
