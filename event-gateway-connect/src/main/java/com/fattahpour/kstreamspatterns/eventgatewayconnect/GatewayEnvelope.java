package com.fattahpour.kstreamspatterns.eventgatewayconnect;

public record GatewayEnvelope(String id, int schemaVersion, String payload, String correlationId) {}
