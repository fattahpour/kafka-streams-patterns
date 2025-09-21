package com.fattahpour.kstreamspatterns.cqrsprojections;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CqrsProjectionsApp {
  private static final Logger LOG = LoggerFactory.getLogger(CqrsProjectionsApp.class);

  private CqrsProjectionsApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-projections-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    KafkaStreams streams = new KafkaStreams(CqrsProjectionsTopology.build(metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping CQRS Projections app");
                  streams.close();
                }));
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        CqrsProjectionsApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
