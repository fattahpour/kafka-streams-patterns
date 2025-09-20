package com.fattahpour.kstreamspatterns.claimcheck;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClaimCheckApp {
  private static final Logger LOG = LoggerFactory.getLogger(ClaimCheckApp.class);

  private ClaimCheckApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "claim-check-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    ClaimCheckStore store = FileSystemClaimCheckStore.defaultStore();
    KafkaStreams streams = new KafkaStreams(ClaimCheckTopology.build(store, metrics), properties);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down Claim Check app");
                  streams.close();
                }));
    LOG.info("Starting Claim Check topology with store at {}", Path.of(System.getProperty("claim.check.store", "/tmp/blobstore")));
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        ClaimCheckApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
