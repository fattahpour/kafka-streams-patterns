package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public interface GatewaySchemaValidator {
  boolean isValid(GatewayEnvelope envelope);
}
