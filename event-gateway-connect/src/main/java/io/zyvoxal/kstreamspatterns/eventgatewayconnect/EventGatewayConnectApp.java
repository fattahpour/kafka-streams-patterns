package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventGatewayConnectApp {
  private static final Logger LOG = LoggerFactory.getLogger(EventGatewayConnectApp.class);

  private EventGatewayConnectApp() {}

  public static void main(String[] args) throws IOException {
    Properties properties = loadProperties();
    properties.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "event-gateway-connect-app");
    properties.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    PatternMetrics metrics = new PatternMetrics();
    GatewaySchemaValidator validator = new SimpleGatewaySchemaValidator(1);
    int maxRetries = Integer.parseInt(properties.getProperty("gateway.retry.max", "3"));
    Duration backoff = Duration.ofMillis(Long.parseLong(properties.getProperty("gateway.retry.backoff.ms", "5000")));

    KafkaStreams streams =
        new KafkaStreams(
            EventGatewayConnectTopology.build(validator, maxRetries, backoff, metrics), properties);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping Event Gateway Connect app");
                  streams.close();
                }));
    LOG.info(
        "Starting Event Gateway Connect topology with retries={} and backoff={}ms",
        maxRetries,
        backoff.toMillis());
    streams.start();
  }

  private static Properties loadProperties() throws IOException {
    Properties properties = new Properties();
    try (InputStream input =
        EventGatewayConnectApp.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input != null) {
        properties.load(input);
      }
    }
    return properties;
  }
}
