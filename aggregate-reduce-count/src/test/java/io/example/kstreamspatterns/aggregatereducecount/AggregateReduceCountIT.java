package io.example.kstreamspatterns.aggregatereducecount;

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

public class AggregateReduceCountIT extends KafkaIntegrationTest {
  @Test
  void endToEnd() {
    System.setProperty("input.topic", "input-it");
    System.setProperty("sum.topic", "sum-it");
    System.setProperty("max.topic", "max-it");
    System.setProperty("count.topic", "count-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "arc-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put("key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put("value.serializer", Serdes.Long().serializer().getClass().getName());
    try (KafkaProducer<String, Long> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-it", "k", 1L));
      producer.send(new ProducerRecord<>("input-it", "k", 2L));
      producer.send(new ProducerRecord<>("input-it", "k", 3L));
      producer.flush();
    }

    assertThat(readLastValue("sum-it", "sum")).isEqualTo(6L);
    assertThat(readLastValue("max-it", "max")).isEqualTo(3L);
    assertThat(readLastValue("count-it", "count")).isEqualTo(3L);

    streams.close();
  }

  private Long readLastValue(String topic, String group) {
    Properties consProps = new Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass().getName());
    consProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Serdes.Long().deserializer().getClass().getName());
    try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consProps)) {
      consumer.subscribe(Collections.singleton(topic));
      ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(10));
      Long val = null;
      for (ConsumerRecord<String, Long> r : records.records(topic)) {
        val = r.value();
      }
      return val;
    }
  }
}
