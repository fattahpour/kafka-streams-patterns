package com.fattahpour.kstreamspatterns.idempotentwriterreader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IdempotentWriterReaderApp {
  private static final Logger LOG = LoggerFactory.getLogger(IdempotentWriterReaderApp.class);

  private IdempotentWriterReaderApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "idempotent-writer-reader-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    PatternMetrics metrics = new PatternMetrics();
    KafkaStreams streams = new KafkaStreams(IdempotentWriterReaderTopology.build(metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping Idempotent Writer/Reader app");
                  streams.close();
                }));
    LOG.info("Starting Idempotent Writer/Reader topology");
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        IdempotentWriterReaderApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
