package com.fattahpour.kstreamspatterns.common;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

/** Utility to seed Kafka topics with sample data for the pattern modules. */
public class FakeDataGenerator {

  private static final String[] MODULES = {
    "branch-route",
    "enrichment-ktable",
    "enrichment-globalktable",
    "join-kstream-kstream",
    "join-kstream-ktable",
    "join-ktable-ktable",
    "aggregate-reduce-count",
    "deduplication",
    "suppression",
    "materialized-views",
    "exactly-once-outbox",
    "retry-dlq",
    "late-early-data",
    "fanout-fanin",
    "rekey-repartition",
    "agg-window-tumbling",
    "agg-window-hopping",
    "agg-window-session",
    "stateless-transforms"
  };

  public static void main(String[] args) throws Exception {
    String[] targets = args.length > 0 ? args : MODULES;

    Properties props = new Properties();
    props.put(
        "bootstrap.servers",
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
    props.put("key.serializer", Serdes.String().serializer().getClass().getName());
    props.put("value.serializer", Serdes.String().serializer().getClass().getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (String pattern : targets) {
        System.out.println("seeding data for " + pattern);
        generateFor(producer, pattern);
      }
    }
  }

  private static void generateFor(KafkaProducer<String, String> producer, String pattern)
      throws Exception {
    switch (pattern) {
      case "branch-route":
        for (int i = 0; i < 10; i++) {
          String key = UUID.randomUUID().toString();
          send(producer, "input-branch", key, Integer.toString(i));
        }
        break;
      case "enrichment-ktable":
      case "enrichment-globalktable":
        send(producer, "products", "p1", "Product-1");
        send(producer, "products", "p2", "Product-2");
        send(producer, "orders", "o1", "p1");
        send(producer, "orders", "o2", "p2");
        break;
      case "join-kstream-kstream":
        for (int i = 0; i < 5; i++) {
          String key = "k" + i;
          send(producer, "left", key, "L" + i);
          send(producer, "right", key, "R" + i);
        }
        break;
      case "join-kstream-ktable":
        send(producer, "table", "k1", "T1");
        send(producer, "table", "k2", "T2");
        send(producer, "stream", "k1", "S1");
        send(producer, "stream", "k2", "S2");
        break;
      case "join-ktable-ktable":
        send(producer, "left-table", "k1", "L1");
        send(producer, "right-table", "k1", "R1");
        send(producer, "left-table", "k2", "L2");
        send(producer, "right-table", "k2", "R2");
        break;
      case "aggregate-reduce-count":
        for (int i = 0; i < 5; i++) {
          String key = "k" + (i % 2);
          send(producer, "arc-input", key, "v" + i);
        }
        break;
      case "deduplication":
        String key = "dup";
        send(producer, "input-dedup", key, "v1");
        Thread.sleep(100);
        send(producer, "input-dedup", key, "v1");
        send(producer, "input-dedup", UUID.randomUUID().toString(), "v2");
        break;
      case "suppression":
        for (int i = 0; i < 5; i++) {
          String keySup = "k" + (i % 2);
          send(producer, "input-suppression", keySup, "v" + i);
        }
        break;
      case "materialized-views":
        for (int i = 0; i < 5; i++) {
          String keyMat = "k" + (i % 2);
          send(producer, "input-materialized", keyMat, "v" + i);
        }
        break;
      case "exactly-once-outbox":
        for (int i = 0; i < 5; i++) {
          String keyOut = "order" + i;
          send(producer, "orders", keyOut, "sku-" + i);
        }
        break;
      case "retry-dlq":
        for (int i = 0; i < 5; i++) {
          String keyRetry = UUID.randomUUID().toString();
          send(producer, "input-retry", keyRetry, "value-" + i);
        }
        break;
      case "late-early-data":
        for (int i = 0; i < 5; i++) {
          String keyLed = "k";
          String value = Long.toString(System.currentTimeMillis() + i * 1000);
          send(producer, "late-early-input", keyLed, value);
        }
        break;
      case "fanout-fanin":
        for (int i = 0; i < 5; i++) {
          String keyFan = UUID.randomUUID().toString();
          send(producer, "fanout-input", keyFan, "v" + i);
        }
        break;
      case "rekey-repartition":
        for (int i = 0; i < 5; i++) {
          String keyRe = "orig" + i;
          send(producer, "input-rekey", keyRe, "val" + i);
        }
        break;
      case "agg-window-tumbling":
        for (int i = 0; i < 5; i++) {
          String keyTum = "k" + (i % 2);
          send(producer, "tumbling-input", keyTum, "v" + i);
        }
        break;
      case "agg-window-hopping":
        for (int i = 0; i < 5; i++) {
          String keyHop = "k" + (i % 2);
          send(producer, "input", keyHop, "v" + i);
        }
        break;
      case "agg-window-session":
        for (int i = 0; i < 5; i++) {
          String keySess = "k";
          send(producer, "session-input", keySess, "v" + i);
        }
        break;
      case "stateless-transforms":
        seedStateless(producer);
        break;
      default:
        seedStateless(producer);
        break;
    }
  }

  private static void seedStateless(KafkaProducer<String, String> producer) throws Exception {
    for (int i = 0; i < 5; i++) {
      String key = UUID.randomUUID().toString();
      send(producer, "input-stateless", key, "value-" + i);
    }
  }

  private static void send(
      KafkaProducer<String, String> producer, String topic, String key, String value) throws Exception {
    RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, value)).get();
    System.out.printf("sent %s:%s to %s@%d%n", key, value, meta.topic(), meta.offset());
    Thread.sleep(Duration.ofMillis(100).toMillis());
  }
}

