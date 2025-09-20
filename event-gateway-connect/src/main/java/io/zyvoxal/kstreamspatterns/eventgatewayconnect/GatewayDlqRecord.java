package io.zyvoxal.kstreamspatterns.eventgatewayconnect;

public record GatewayDlqRecord(String id, String reason, String correlationId, int attempt) {}
