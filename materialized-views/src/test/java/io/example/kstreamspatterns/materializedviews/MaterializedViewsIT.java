package io.example.kstreamspatterns.materializedviews;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.example.kstreamspatterns.common.KafkaIntegrationTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class MaterializedViewsIT extends KafkaIntegrationTest {
  @Test
  void queryMaterializedStore() throws Exception {
    System.setProperty("input.topic", "input-materialized-it");
    System.setProperty("output.topic", "output-materialized-it");
    System.setProperty("http.port", "8081");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-views-it");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    streams.start();
    HttpServer server = RestService.start(streams);

    Properties prodProps = new Properties();
    prodProps.put("bootstrap.servers", bootstrapServers());
    prodProps.put(
        "key.serializer", Serdes.String().serializer().getClass().getName());
    prodProps.put(
        "value.serializer", Serdes.String().serializer().getClass().getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
      producer.send(new ProducerRecord<>("input-materialized-it", "k1", "1"));
      producer.flush();
    }

    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:8081/count/k1")).build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.body()).isEqualTo("1");

    server.stop(0);
    streams.close();
  }
}
