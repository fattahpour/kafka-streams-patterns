package com.fattahpour.kstreamspatterns.sagaorchestration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SagaOrchestrationApp {
  private static final Logger LOG = LoggerFactory.getLogger(SagaOrchestrationApp.class);

  private SagaOrchestrationApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "saga-orchestration-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    KafkaStreams streams = new KafkaStreams(SagaOrchestrationTopology.build(metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping Saga Orchestration app");
                  streams.close();
                }));
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        SagaOrchestrationApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
