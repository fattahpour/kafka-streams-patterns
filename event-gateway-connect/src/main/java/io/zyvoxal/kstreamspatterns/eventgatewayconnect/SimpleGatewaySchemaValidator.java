package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public final class SimpleGatewaySchemaValidator implements GatewaySchemaValidator {
  private final int expectedVersion;

  public SimpleGatewaySchemaValidator(int expectedVersion) {
    this.expectedVersion = expectedVersion;
  }

  @Override
  public boolean isValid(GatewayEnvelope envelope) {
    return envelope != null
        && envelope.schemaVersion() == expectedVersion
        && envelope.payload() != null
        && !envelope.payload().isBlank();
  }
}
