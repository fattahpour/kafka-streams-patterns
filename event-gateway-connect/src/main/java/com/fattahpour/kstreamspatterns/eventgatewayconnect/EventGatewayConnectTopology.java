package com.fattahpour.kstreamspatterns.eventgatewayconnect;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public final class EventGatewayConnectTopology {
  private static final String DEFAULT_INPUT = "pattern.event-gateway-connect.in";
  private static final String DEFAULT_OUTPUT = "pattern.event-gateway-connect.out";
  private static final String DEFAULT_RETRY = "pattern.event-gateway-connect.retry";
  private static final String DEFAULT_DLQ = "pattern.event-gateway-connect.dlq";

  private EventGatewayConnectTopology() {}

  public static Topology build(
      GatewaySchemaValidator validator, int maxRetries, Duration backoff, PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<GatewayEnvelope> envelopeSerde = JsonSerdes.serde(GatewayEnvelope.class);
    Serde<GatewayDispatch> dispatchSerde = JsonSerdes.serde(GatewayDispatch.class);
    Serde<GatewayDlqRecord> dlqSerde = JsonSerdes.serde(GatewayDlqRecord.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String output = System.getProperty("output.topic", DEFAULT_OUTPUT);
    String retry = System.getProperty("retry.topic", DEFAULT_RETRY);
    String dlq = System.getProperty("dlq.topic", DEFAULT_DLQ);

    KStream<String, GatewayEnvelope> source =
        builder.stream(input, Consumed.with(Serdes.String(), envelopeSerde));

    KStream<String, GatewayEnvelope>[] schemaBranches =
        source.branch((key, value) -> validator.isValid(value), (key, value) -> true);

    KStream<String, GatewayEnvelope> valid = schemaBranches[0];
    KStream<String, GatewayEnvelope> invalid = schemaBranches[1];

    invalid
        .mapValues(
            value ->
                value == null
                    ? new GatewayDlqRecord(null, "schema-invalid", null, 0)
                    : new GatewayDlqRecord(value.id(), "schema-invalid", value.correlationId(), 0))
        .peek((key, value) -> metrics.markDlq())
        .to(dlq, Produced.with(Serdes.String(), dlqSerde));

    GatewayProcessor processor = new GatewayProcessor(maxRetries, backoff, metrics);
    KStream<String, GatewayProcessingResult> processed =
        valid.transformValues(
            new GatewayProcessingTransformer(processor), Named.as("gateway-processor"));

    KStream<String, GatewayProcessingResult>[] branches =
        processed.branch(
            (key, value) -> value != null && value.status() == GatewayProcessingResult.Status.SUCCESS,
            (key, value) -> value != null && value.status() == GatewayProcessingResult.Status.RETRY,
            (key, value) -> value != null && value.status() == GatewayProcessingResult.Status.DLQ);

    KStream<String, GatewayProcessingResult> success = branches[0];
    KStream<String, GatewayProcessingResult> retries = branches[1];
    KStream<String, GatewayProcessingResult> failures = branches[2];

    success
        .mapValues(result ->
            new GatewayDispatch(
                result.envelope().id(), result.envelope().payload(), result.envelope().correlationId(), result.attempt()))
        .to(output, Produced.with(Serdes.String(), dispatchSerde));

    retries
        .mapValues(GatewayProcessingResult::envelope)
        .to(retry, Produced.with(Serdes.String(), envelopeSerde));

    failures
        .mapValues(
            result ->
                new GatewayDlqRecord(
                    result.envelope().id(), result.reason(), result.envelope().correlationId(), result.attempt()))
        .to(dlq, Produced.with(Serdes.String(), dlqSerde));

    return builder.build();
  }
}
