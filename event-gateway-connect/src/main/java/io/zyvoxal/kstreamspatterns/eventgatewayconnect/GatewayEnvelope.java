package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public record GatewayEnvelope(String id, int schemaVersion, String payload, String correlationId) {}
