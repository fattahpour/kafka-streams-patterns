package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public record GatewayDispatch(String id, String payload, String correlationId, int attempt) {}
