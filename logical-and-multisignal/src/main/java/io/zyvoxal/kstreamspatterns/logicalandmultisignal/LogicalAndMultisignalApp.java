package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogicalAndMultisignalApp {
  private static final Logger LOG = LoggerFactory.getLogger(LogicalAndMultisignalApp.class);

  private LogicalAndMultisignalApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "logical-and-multisignal-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    KafkaStreams streams = new KafkaStreams(LogicalAndMultisignalTopology.build(metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping Logical AND Multisignal app");
                  streams.close();
                }));
    LOG.info(
        "Starting Logical AND Multisignal topology with window {} ms",
        properties.getProperty("correlation.window.ms", "60000"));
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        LogicalAndMultisignalApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
