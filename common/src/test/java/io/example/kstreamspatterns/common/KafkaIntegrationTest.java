package io.example.kstreamspatterns.common;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class KafkaIntegrationTest {
  protected static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

  @BeforeAll
  static void start() {
    kafka.start();
  }

  @AfterAll
  static void stop() {
    kafka.stop();
  }

  protected String bootstrapServers() {
    return kafka.getBootstrapServers();
  }
}
