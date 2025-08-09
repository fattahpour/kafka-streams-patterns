package io.example.kstreamspatterns.materializedviews;

import com.sun.net.httpserver.HttpServer;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public class App {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.putAll(System.getProperties());
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getProperty("application.id"));
    props.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers"));

    KafkaStreams streams = new KafkaStreams(TopologyBuilder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    streams.start();

    HttpServer server = RestService.start(streams);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
  }
}
