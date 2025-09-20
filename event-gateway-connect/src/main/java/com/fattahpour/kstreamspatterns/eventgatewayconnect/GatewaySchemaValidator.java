package com.fattahpour.kstreamspatterns.eventgatewayconnect;

public interface GatewaySchemaValidator {
  boolean isValid(GatewayEnvelope envelope);
}
