package com.fattahpour.kstreamspatterns.idempotentwriterreader;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

public final class IdempotentWriterReaderTopology {
  private static final String DEFAULT_INPUT = "pattern.idempotent-writer-reader.in";
  private static final String DEFAULT_WRITER = "pattern.idempotent-writer-reader.writer";
  private static final String DEFAULT_OUTPUT = "pattern.idempotent-writer-reader.out";
  private static final String DEFAULT_DLQ = "pattern.idempotent-writer-reader.dlq";

  private IdempotentWriterReaderTopology() {}

  public static Topology build(PatternMetrics metrics) {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<InboundEvent> inboundSerde = JsonSerdes.serde(InboundEvent.class);
    Serde<DeduplicatedEvent> dedupSerde = JsonSerdes.serde(DeduplicatedEvent.class);
    Serde<ProcessedEvent> processedSerde = JsonSerdes.serde(ProcessedEvent.class);
    Serde<DlqRecord> dlqSerde = JsonSerdes.serde(DlqRecord.class);

    String input = System.getProperty("input.topic", DEFAULT_INPUT);
    String writerTopic = System.getProperty("writer.topic", DEFAULT_WRITER);
    String output = System.getProperty("output.topic", DEFAULT_OUTPUT);
    String dlq = System.getProperty("dlq.topic", DEFAULT_DLQ);

    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(WriterDeduplicationTransformer.STORE_NAME),
            Serdes.String(),
            Serdes.Long()));
    builder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(ReaderDeduplicationTransformer.STORE_NAME),
            Serdes.String(),
            Serdes.Long()));

    KStream<String, InboundEvent> source =
        builder.stream(input, Consumed.with(Serdes.String(), inboundSerde));

    KStream<String, InboundEvent>[] branches =
        source.branch(
            (key, value) -> value != null && value.eventId() != null && !value.eventId().isBlank(),
            (key, value) -> true);
    KStream<String, InboundEvent> valid = branches[0];
    KStream<String, InboundEvent> invalid = branches[1];

    invalid
        .mapValues(value -> new DlqRecord(value != null ? value.eventId() : null, "missing-event-id", value != null ? value.correlationId() : null))
        .peek((key, value) -> metrics.markDlq())
        .to(dlq, Produced.with(Serdes.String(), dlqSerde));

    KStream<String, DeduplicatedEvent> writer =
        valid
            .transformValues(
                new WriterDeduplicationTransformer(metrics),
                Named.as("writer-dedup"),
                WriterDeduplicationTransformer.STORE_NAME)
            .filter((key, value) -> value != null)
            .selectKey((key, value) -> value.eventId());

    writer.to(writerTopic, Produced.with(Serdes.String(), dedupSerde));

    KStream<String, DeduplicatedEvent> writerStream =
        builder.stream(writerTopic, Consumed.with(Serdes.String(), dedupSerde));

    KStream<String, ProcessedEvent> reader =
        writerStream
            .transformValues(
                new ReaderDeduplicationTransformer(metrics),
                Named.as("reader-dedup"),
                ReaderDeduplicationTransformer.STORE_NAME)
            .filter((key, value) -> value != null);

    reader.to(output, Produced.with(Serdes.String(), processedSerde));

    return builder.build();
  }
}
