package com.fattahpour.kstreamspatterns.claimcheck;

import com.fattahpour.kstreamspatterns.common.JsonSerdes;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public final class ClaimCheckTopology {
    private static final String DEFAULT_INPUT = "pattern.claim-check.in";
    private static final String DEFAULT_REFERENCE_TOPIC = "pattern.claim-check.refs";
    private static final String DEFAULT_OUTPUT = "pattern.claim-check.out";
    private static final String DEFAULT_DLQ = "pattern.claim-check.dlq";

    private ClaimCheckTopology() {
    }

    public static Topology build(ClaimCheckStore store, PatternMetrics metrics) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<InboundDocument> documentSerde = JsonSerdes.serde(InboundDocument.class);
        Serde<ClaimCheckReference> referenceSerde = JsonSerdes.serde(ClaimCheckReference.class);
        Serde<ResolvedDocument> resolvedSerde = JsonSerdes.serde(ResolvedDocument.class);
        Serde<ClaimFailure> failureSerde = JsonSerdes.serde(ClaimFailure.class);

        String input = System.getProperty("input.topic", DEFAULT_INPUT);
        String referencesTopic = System.getProperty("references.topic", DEFAULT_REFERENCE_TOPIC);
        String output = System.getProperty("output.topic", DEFAULT_OUTPUT);
        String dlq = System.getProperty("dlq.topic", DEFAULT_DLQ);

        KStream<String, InboundDocument> inputStream =
                builder.stream(input, Consumed.with(Serdes.String(), documentSerde));

        KStream<String, InboundDocument>[] branches =
                inputStream.branch(
                        (key, value) -> value != null && value.payload() != null && !value.payload().isBlank(),
                        (key, value) -> true);
        KStream<String, InboundDocument> valid = branches[0];
        KStream<String, InboundDocument> invalid = branches[1];

        invalid
                .map(
                        (key, value) ->
                                KeyValue.pair(
                                        key,
                                        new ClaimFailure(
                                                value != null ? value.id() : key,
                                                "payload-missing",
                                                value != null ? value.correlationId() : null)))
                .peek((key, value) -> metrics.markDlq())
                .to(dlq, Produced.with(Serdes.String(), failureSerde));

        KStream<String, ClaimCheckReference> references =
                valid.transform(
                        new ClaimWriterTransformerSupplier(store, metrics),
                        Named.as("claim-check-writer"));

        references
                .peek((key, value) -> metrics.markReference())
                .to(referencesTopic, Produced.with(Serdes.String(), referenceSerde));

        KStream<String, ClaimCheckReference> referencesThrough =
                builder.stream(referencesTopic, Consumed.with(Serdes.String(), referenceSerde));

        KStream<String, ResolvedDocument> resolved =
                referencesThrough.transformValues(
                        new ClaimResolverTransformerSupplier(store, metrics),
                        Named.as("claim-check-resolver"));

        resolved.to(output, Produced.with(Serdes.String(), resolvedSerde));

        return builder.build();
    }

    static final class ClaimWriterTransformerSupplier
            implements TransformerSupplier<String, InboundDocument, KeyValue<String, ClaimCheckReference>> {
        private final ClaimCheckStore store;
        private final PatternMetrics metrics;

        ClaimWriterTransformerSupplier(ClaimCheckStore store, PatternMetrics metrics) {
            this.store = store;
            this.metrics = metrics;
        }

        @Override
        public Transformer<String, InboundDocument, KeyValue<String, ClaimCheckReference>> get() {
            return new ClaimWriterTransformer(store, metrics);
        }
    }

    static final class ClaimWriterTransformer
            implements Transformer<String, InboundDocument, KeyValue<String, ClaimCheckReference>> {
        private final ClaimCheckStore store;
        private final PatternMetrics metrics;
        private ProcessorContext context;

        ClaimWriterTransformer(ClaimCheckStore store, PatternMetrics metrics) {
            this.store = store;
            this.metrics = metrics;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, ClaimCheckReference> transform(String key, InboundDocument value) {
            metrics.markProcessed();
            if (value == null) {
                return null;
            }
            Headers headers = context.headers();
            headers.add("correlation-id", header(value.correlationId()));
            headers.add("causation-id", header(value.id()));
            URI uri = store.put(value.id(), value.payload().getBytes(StandardCharsets.UTF_8));
            ClaimCheckReference reference = new ClaimCheckReference(value.id(), uri, value.correlationId());
            headers.add("claim-check-uri", header(uri.toString()));
            return KeyValue.pair(key, reference);
        }

        @Override
        public void close() {
        }

        private byte[] header(String value) {
            return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        }
    }

    static final class ClaimResolverTransformerSupplier
            implements ValueTransformerWithKeySupplier<String, ClaimCheckReference, ResolvedDocument> {
        private final ClaimCheckStore store;
        private final PatternMetrics metrics;

        ClaimResolverTransformerSupplier(ClaimCheckStore store, PatternMetrics metrics) {
            this.store = store;
            this.metrics = metrics;
        }

        @Override
        public ValueTransformerWithKey<String, ClaimCheckReference, ResolvedDocument> get() {
            return new Resolver(store, metrics);
        }
    }

    static final class Resolver
            implements ValueTransformerWithKey<String, ClaimCheckReference, ResolvedDocument> {
        private final ClaimCheckStore store;
        private final PatternMetrics metrics;
        private ProcessorContext context;

        Resolver(ClaimCheckStore store, PatternMetrics metrics) {
            this.store = store;
            this.metrics = metrics;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public ResolvedDocument transform(String readOnlyKey, ClaimCheckReference value) {
            if (value == null) {
                return null;
            }
            Optional<byte[]> payloadBytes = store.get(value.uri());
            if (payloadBytes.isPresent()) {
                context.headers().add("claim-check-hit", "true".getBytes(StandardCharsets.UTF_8));
                metrics.markResolved();
                return new ResolvedDocument(
                        value.id(), new String(payloadBytes.get(), StandardCharsets.UTF_8), false, value.correlationId());
            }
            context.headers().add("claim-check-hit", "false".getBytes(StandardCharsets.UTF_8));
            metrics.markFallback();
            return new ResolvedDocument(
                    value.id(),
                    "payload-missing",
                    true,
                    value.correlationId());
        }

        @Override
        public void close() {
        }
    }
}
