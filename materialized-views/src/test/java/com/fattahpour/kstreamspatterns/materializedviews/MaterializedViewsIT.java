package com.fattahpour.kstreamspatterns.materializedviews;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MaterializedViewsIT {

    @Test
    void queryMaterializedStore() {
        // Align with builder defaults (can still be overridden by -D props)
        System.setProperty("input.topic", "input-materialized");
        System.setProperty("output.topic", "output-materialized");
        System.setProperty("values.store", "values-store");
        System.setProperty("counts.store", "counts-store");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-views-it");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(TopologyBuilder.build(), props)) {
            TestInputTopic<String, String> in =
                    driver.createInputTopic("input-materialized", Serdes.String().serializer(), Serdes.String().serializer());

            // Write some data
            in.pipeInput("k1", "v1");
            in.pipeInput("k1", "v2"); // latest wins in values store
            in.pipeInput("k2", "z");

            // Query materialized "latest value" view
            @SuppressWarnings("unchecked")
            KeyValueStore<String, String> values =
                    driver.getKeyValueStore("values-store");
            assertThat(values.get("k1")).isEqualTo("v2");
            assertThat(values.get("k2")).isEqualTo("z");

            // Query materialized counts view
            @SuppressWarnings("unchecked")
            KeyValueStore<String, Long> counts =
                    driver.getKeyValueStore("counts-store");
            assertThat(counts.get("k1")).isEqualTo(2L);
            assertThat(counts.get("k2")).isEqualTo(1L);
        }
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("input.topic");
        System.clearProperty("output.topic");
        System.clearProperty("values.store");
        System.clearProperty("counts.store");
    }
}
