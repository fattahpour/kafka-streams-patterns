package io.example.kstreamspatterns.branchroute;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.Arrays;
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

public class BranchRouteIT extends KafkaIntegrationTest {
  @Test
  void endToEndBranching() {
    System.setProperty("input.topic", "input-branch-it");
    System.setProperty("even.topic", "even-branch-it");
    System.setProperty("odd.topic", "odd-branch-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-route-it");
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
      producer.send(new ProducerRecord<>("input-branch-it", "k1", "1"));
      producer.send(new ProducerRecord<>("input-branch-it", "k2", "2"));
      producer.send(new ProducerRecord<>("input-branch-it", "k3", "3"));
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
      consumer.subscribe(Arrays.asList("even-branch-it", "odd-branch-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      List<String> evenValues =
          java.util.stream.StreamSupport
              .stream(records.records("even-branch-it").spliterator(), false)
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());
      List<String> oddValues =
          java.util.stream.StreamSupport
              .stream(records.records("odd-branch-it").spliterator(), false)
              .map(ConsumerRecord::value)
              .collect(Collectors.toList());
      assertThat(evenValues).containsExactly("2");
      assertThat(oddValues).containsExactlyInAnyOrder("1", "3");
    }

    streams.close();
  }
}
