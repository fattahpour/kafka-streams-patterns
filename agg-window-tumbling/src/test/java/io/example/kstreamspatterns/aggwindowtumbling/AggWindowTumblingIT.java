package io.example.kstreamspatterns.aggwindowtumbling;

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
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class AggWindowTumblingIT extends KafkaIntegrationTest {
  @Test
  void endToEndAggregation() {
    System.setProperty("input.topic", "tumbling-input-it");
    System.setProperty("output.topic", "tumbling-output-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-window-tumbling-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put(
        "key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put(
        "value.serializer", Serdes.String().serializer().getClass().getName());
    long now = System.currentTimeMillis();
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("tumbling-input-it", null, now, "k1", "v1"));
      producer.send(new ProducerRecord<>("tumbling-input-it", null, now + 1000, "k1", "v2"));
      producer.send(
          new ProducerRecord<>(
              "tumbling-input-it", null, now + Duration.ofMinutes(1).toMillis() + 1, "k1", "v3"));
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
      consumer.subscribe(Collections.singletonList("tumbling-output-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      List<String> values = new ArrayList<>();
      records.forEach(r -> values.add(r.value()));
      assertThat(values).containsExactly("1", "2", "1");
    }

    streams.close();
  }
}
