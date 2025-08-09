package io.example.kstreamspatterns.retrydlq;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public final class App {
  private App() {}

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        System.getProperty("application.id", "retry-dlq-app"));
    props.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        System.getProperty("bootstrap.servers", "localhost:9092"));
    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
