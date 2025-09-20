package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class LogicalAndMultisignalTopologyTest {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<SignalEnvelope> ENVELOPE_SERDE = JsonSerdes.serde(SignalEnvelope.class);
  private static final Serde<CorrelatedSignal> CORRELATED_SERDE = JsonSerdes.serde(CorrelatedSignal.class);
  private static final Serde<ExpiredCorrelation> EXPIRED_SERDE = JsonSerdes.serde(ExpiredCorrelation.class);

  @Test
  void emitsWhenAllSignalsArrive() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = LogicalAndMultisignalTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, SignalEnvelope> input =
          driver.createInputTopic(
              "pattern.logical-and-multisignal.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, CorrelatedSignal> output =
          driver.createOutputTopic(
              "pattern.logical-and-multisignal.out", STRING_SERDE.deserializer(), CORRELATED_SERDE.deserializer());

      input.pipeInput("order-1", new SignalEnvelope("order-1", "A", "alpha", "corr-1"));
      input.pipeInput("order-1", new SignalEnvelope("order-1", "B", "beta", "corr-1"));
      input.pipeInput("order-1", new SignalEnvelope("order-1", "C", "gamma", "corr-1"));

      CorrelatedSignal correlated = output.readValue();
      assertThat(correlated.payloads()).containsAllEntriesOf(Map.of(SignalType.A, "alpha", SignalType.B, "beta", SignalType.C, "gamma"));
      assertThat(metrics.completed()).isEqualTo(1);
    }
  }

  @Test
  void expiresWhenSignalsMissing() {
    System.setProperty("correlation.window.ms", "500");
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = LogicalAndMultisignalTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, SignalEnvelope> input =
          driver.createInputTopic(
              "pattern.logical-and-multisignal.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, ExpiredCorrelation> expired =
          driver.createOutputTopic(
              "pattern.logical-and-multisignal.expired", STRING_SERDE.deserializer(), EXPIRED_SERDE.deserializer());

      input.pipeInput("order-2", new SignalEnvelope("order-2", "A", "alpha", "corr-2"));
      input.pipeInput("order-2", new SignalEnvelope("order-2", "B", "beta", "corr-2"));
      driver.advanceWallClockTime(Duration.ofMillis(600));

      ExpiredCorrelation correlation = expired.readValue();
      assertThat(correlation.missingSignals()).containsExactly(SignalType.C);
      assertThat(metrics.expired()).isEqualTo(1);
    } finally {
      System.clearProperty("correlation.window.ms");
    }
  }

  @Test
  void duplicateSignalIgnored() {
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = LogicalAndMultisignalTopology.build(metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, SignalEnvelope> input =
          driver.createInputTopic(
              "pattern.logical-and-multisignal.in", STRING_SERDE.serializer(), ENVELOPE_SERDE.serializer());
      TestOutputTopic<String, CorrelatedSignal> output =
          driver.createOutputTopic(
              "pattern.logical-and-multisignal.out", STRING_SERDE.deserializer(), CORRELATED_SERDE.deserializer());

      input.pipeInput("order-3", new SignalEnvelope("order-3", "A", "alpha", "corr-3"));
      input.pipeInput("order-3", new SignalEnvelope("order-3", "A", "alpha-dup", "corr-3"));
      input.pipeInput("order-3", new SignalEnvelope("order-3", "B", "beta", "corr-3"));
      input.pipeInput("order-3", new SignalEnvelope("order-3", "C", "gamma", "corr-3"));

      CorrelatedSignal correlated = output.readValue();
      assertThat(correlated.payloads().get(SignalType.A)).isEqualTo("alpha");
      assertThat(output.isEmpty()).isTrue();
      assertThat(metrics.completed()).isEqualTo(1);
    }
  }

  private Properties props() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "logical-and-multisignal-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    return properties;
  }
}
