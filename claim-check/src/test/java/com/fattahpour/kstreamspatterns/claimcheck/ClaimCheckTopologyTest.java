package com.fattahpour.kstreamspatterns.claimcheck;

import static org.assertj.core.api.Assertions.assertThat;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class ClaimCheckTopologyTest {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<InboundDocument> INBOUND_SERDE = JsonSerdes.serde(InboundDocument.class);
  private static final Serde<ClaimCheckReference> REFERENCE_SERDE = JsonSerdes.serde(ClaimCheckReference.class);
  private static final Serde<ResolvedDocument> RESOLVED_SERDE = JsonSerdes.serde(ResolvedDocument.class);
  private static final Serde<ClaimFailure> FAILURE_SERDE = JsonSerdes.serde(ClaimFailure.class);

  @Test
  void largePayloadStoredAndReferencePublished() {
    ClaimCheckStore store = new InMemoryClaimCheckStore();
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ClaimCheckTopology.build(store, metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundDocument> input =
          driver.createInputTopic("pattern.claim-check.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ClaimCheckReference> references =
          driver.createOutputTopic(
              "pattern.claim-check.refs", STRING_SERDE.deserializer(), REFERENCE_SERDE.deserializer());

      String largePayload = "X".repeat(2048);
      input.pipeInput("doc-1", new InboundDocument("doc-1", largePayload, "corr-1", "text/plain"));

      ClaimCheckReference ref = references.readValue();
      assertThat(ref.uri().toString()).doesNotContain(largePayload);
      assertThat(metrics.references()).isEqualTo(1);
      assertThat(metrics.processed()).isEqualTo(1);
    }
  }

  @Test
  void resolvesPayloadFromStore() {
    ClaimCheckStore store = new InMemoryClaimCheckStore();
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ClaimCheckTopology.build(store, metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundDocument> input =
          driver.createInputTopic("pattern.claim-check.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ResolvedDocument> output =
          driver.createOutputTopic(
              "pattern.claim-check.out", STRING_SERDE.deserializer(), RESOLVED_SERDE.deserializer());

      String payload = "important-document";
      input.pipeInput("doc-42", new InboundDocument("doc-42", payload, "corr-42", "text/plain"));

      ResolvedDocument resolved = output.readValue();
      assertThat(resolved.fallbackUsed()).isFalse();
      assertThat(resolved.payload()).isEqualTo(payload);
      assertThat(metrics.resolved()).isEqualTo(1);
    }
  }

  @Test
  void missingPayloadFallsBack() {
    ClaimCheckStore store =
        new ClaimCheckStore() {
          @Override
          public URI put(String id, byte[] payload) {
            return URI.create("memory://missing/" + id);
          }

          @Override
          public Optional<byte[]> get(URI uri) {
            return Optional.empty();
          }
        };
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ClaimCheckTopology.build(store, metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundDocument> input =
          driver.createInputTopic("pattern.claim-check.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ResolvedDocument> output =
          driver.createOutputTopic(
              "pattern.claim-check.out", STRING_SERDE.deserializer(), RESOLVED_SERDE.deserializer());

      input.pipeInput("doc-404", new InboundDocument("doc-404", "payload", "corr-404", "text/plain"));

      ResolvedDocument resolved = output.readValue();
      assertThat(resolved.fallbackUsed()).isTrue();
      assertThat(resolved.payload()).isEqualTo("payload-missing");
      assertThat(metrics.fallbacks()).isEqualTo(1);
    }
  }

  @Test
  void invalidDocumentSentToDlq() {
    ClaimCheckStore store = new InMemoryClaimCheckStore();
    PatternMetrics metrics = new PatternMetrics();
    Topology topology = ClaimCheckTopology.build(store, metrics);

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props())) {
      TestInputTopic<String, InboundDocument> input =
          driver.createInputTopic("pattern.claim-check.in", STRING_SERDE.serializer(), INBOUND_SERDE.serializer());
      TestOutputTopic<String, ClaimFailure> dlq =
          driver.createOutputTopic(
              "pattern.claim-check.dlq", STRING_SERDE.deserializer(), FAILURE_SERDE.deserializer());

      input.pipeInput("doc-2", new InboundDocument("doc-2", null, "corr-2", "text/plain"));

      ClaimFailure failure = dlq.readValue();
      assertThat(failure.reason()).isEqualTo("payload-missing");
      assertThat(metrics.dlq()).isEqualTo(1);
    }
  }

  private Properties props() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "claim-check-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    return properties;
  }
}
