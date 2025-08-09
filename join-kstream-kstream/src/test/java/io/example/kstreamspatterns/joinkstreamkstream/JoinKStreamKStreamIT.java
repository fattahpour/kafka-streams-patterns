package io.example.kstreamspatterns.joinkstreamkstream;

import static org.assertj.core.api.Assertions.assertThat;

import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.time.Duration;
import java.util.Collections;
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

public class JoinKStreamKStreamIT extends KafkaIntegrationTest {
  @Test
  void endToEndJoin() {
    System.setProperty("left.topic", "left-join-it");
    System.setProperty("right.topic", "right-join-it");
    System.setProperty("output.topic", "joined-it");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-kstream-kstream-it");
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
    try (KafkaProducer<String, String> leftProd = new KafkaProducer<>(prodProps);
        KafkaProducer<String, String> rightProd = new KafkaProducer<>(prodProps)) {
      leftProd.send(new ProducerRecord<>("left-join-it", null, now, "k1", "L1"));
      rightProd.send(new ProducerRecord<>("right-join-it", null, now + 1000, "k1", "R1"));
      leftProd.send(new ProducerRecord<>("left-join-it", null, now, "k2", "L2"));
      leftProd.flush();
      rightProd.flush();
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
      consumer.subscribe(Collections.singletonList("joined-it"));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      assertThat(records.count()).isEqualTo(1);
      assertThat(records.iterator().next().value()).isEqualTo("L1:R1");
    }

    streams.close();
  }
}
