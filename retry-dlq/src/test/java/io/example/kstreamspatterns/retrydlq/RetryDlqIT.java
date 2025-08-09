package io.example.kstreamspatterns.retrydlq;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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

public class RetryDlqIT extends KafkaIntegrationTest {
  @Test
  void messageRetriesThenGoesToDlq() {
    System.setProperty("input.topic", "input-retry-it");
    System.setProperty("retry1.topic", "retry1-retry-it");
    System.setProperty("retry2.topic", "retry2-retry-it");
    System.setProperty("dlq.topic", "dlq-retry-it");
    System.setProperty("output.topic", "output-retry-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "retry-dlq-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put("key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put("value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-retry-it", "k1", "fail"));
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
      // first retry
      consumer.subscribe(List.of("retry1-retry-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      ConsumerRecord<String, String> first = records.records("retry1-retry-it").get(0);
      assertThat(
              new String(first.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("1");

      // send to second retry
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
        ProducerRecord<String, String> r1 =
            new ProducerRecord<>("retry1-retry-it", "k1", "fail");
        r1.headers().add("attempt", "1".getBytes(StandardCharsets.UTF_8));
        producer.send(r1);
        producer.flush();
      }

      consumer.subscribe(List.of("retry2-retry-it"));
      records = consumer.poll(Duration.ofSeconds(10));
      ConsumerRecord<String, String> second = records.records("retry2-retry-it").get(0);
      assertThat(
              new String(second.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("2");

      // send to dlq
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
        ProducerRecord<String, String> r2 =
            new ProducerRecord<>("retry2-retry-it", "k1", "fail");
        r2.headers().add("attempt", "2".getBytes(StandardCharsets.UTF_8));
        producer.send(r2);
        producer.flush();
      }

      consumer.subscribe(List.of("dlq-retry-it"));
      records = consumer.poll(Duration.ofSeconds(10));
      ConsumerRecord<String, String> dead = records.records("dlq-retry-it").get(0);
      assertThat(
              new String(dead.headers().lastHeader("attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("3");
      assertThat(new String(dead.headers().lastHeader("error").value(), StandardCharsets.UTF_8))
          .isEqualTo("boom");
    }

    streams.close();
  }
}
