package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class EventGatewayConnectTopologyTest {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<GatewayEnvelope> ENVELOPE_SERDE = JsonSerdes.serde(GatewayEnvelope.class);
  private static final Serde<GatewayDispatch> DISPATCH_SERDE = JsonSerdes.serde(GatewayDispatch.class);
  private static final Serde<GatewayDlqRecord> DLQ_SERDE = JsonSerdes.serde(GatewayDlqRecord.class);

  @Test
  void validRecordRoutesToOutput() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology =
        EventGatewayConnectTopology.build(new SimpleGatewaySchemaValidator(1), 3, Duration.ofSeconds(1), metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, GatewayEnvelope> input =
          driver.createInputTopic(
              "pattern.event-gateway-connect.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, GatewayDispatch> output =
          driver.createOutputTopic(
              "pattern.event-gateway-connect.out", STRING_SERDE.deserializer(), DISPATCH_SERDE.deserializer());

      GatewayEnvelope envelope = new GatewayEnvelope("evt-1", 1, "PAYLOAD", "corr-1");
      input.pipeInput("evt-1", envelope);

      GatewayDispatch dispatch = output.readValue();
      assertThat(dispatch.payload()).isEqualTo("PAYLOAD");
      assertThat(metrics.success()).isEqualTo(1);
    }
  }

  @Test
  void invalidSchemaGoesToDlq() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology =
        EventGatewayConnectTopology.build(new SimpleGatewaySchemaValidator(1), 3, Duration.ofSeconds(1), metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, GatewayEnvelope> input =
          driver.createInputTopic(
              "pattern.event-gateway-connect.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, GatewayDlqRecord> dlq =
          driver.createOutputTopic(
              "pattern.event-gateway-connect.dlq", STRING_SERDE.deserializer(), DLQ_SERDE.deserializer());

      GatewayEnvelope envelope = new GatewayEnvelope("evt-2", 2, "PAYLOAD", "corr-2");
      input.pipeInput("evt-2", envelope);

      GatewayDlqRecord record = dlq.readValue();
      assertThat(record.reason()).isEqualTo("schema-invalid");
      assertThat(metrics.dlq()).isEqualTo(1);
    }
  }

  @Test
  void temporaryFailureRetriedWithBackoffHeader() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology =
        EventGatewayConnectTopology.build(new SimpleGatewaySchemaValidator(1), 2, Duration.ofMillis(500), metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, GatewayEnvelope> input =
          driver.createInputTopic(
              "pattern.event-gateway-connect.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, GatewayEnvelope> retry =
          driver.createOutputTopic(
              "pattern.event-gateway-connect.retry", STRING_SERDE.deserializer(), ENVELOPE_SERDE.deserializer());

      GatewayEnvelope envelope = new GatewayEnvelope("evt-3", 1, "TEMP_ERROR", "corr-3");
      input.pipeInput("evt-3", envelope);

      TestRecord<String, GatewayEnvelope> retryRecord = retry.readRecord();
      assertThat(new String(retryRecord.headers().lastHeader("retry-backoff-ms").value(), StandardCharsets.UTF_8))
          .isEqualTo("500");
      assertThat(new String(retryRecord.headers().lastHeader("retry-attempt").value(), StandardCharsets.UTF_8))
          .isEqualTo("1");
      assertThat(metrics.retries()).isEqualTo(1);
    }
  }

  private Properties props() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-gateway-connect-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    return properties;
  }
}
