package io.zyvoxal.kstreamspatterns.eventchunking;

public record ReassembledEvent(String id, String payload, String correlationId) {}
