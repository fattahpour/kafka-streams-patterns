package io.zyvoxal.kstreamspatterns.eventchunking;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventChunkingApp {
  private static final Logger LOG = LoggerFactory.getLogger(EventChunkingApp.class);

  private EventChunkingApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "event-chunking-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    KafkaStreams streams = new KafkaStreams(EventChunkingTopology.build(metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping Event Chunking app");
                  streams.close();
                }));
    LOG.info("Starting Event Chunking topology with input {}", properties.getProperty("input.topic", "pattern.event-chunking.in"));
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        EventChunkingApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
