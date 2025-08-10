package com.fattahpour.kstreamspatterns.materializedviews;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class RestService {
  public static HttpServer start(KafkaStreams streams) throws IOException {
    int port = Integer.getInteger("http.port", 8080);
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(
        "/count",
        exchange -> {
          String[] parts = exchange.getRequestURI().getPath().split("/");
          if (parts.length < 3) {
            exchange.sendResponseHeaders(400, 0);
            exchange.close();
            return;
          }
          String key = parts[2];
          ReadOnlyKeyValueStore<String, Long> store =
              streams.store(
                  StoreQueryParameters.fromNameAndType(
                      "counts-store", QueryableStoreTypes.keyValueStore()));
          Long value = store.get(key);
          byte[] response =
              String.valueOf(value == null ? 0 : value).getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
        });
    server.start();
    return server;
  }
}
