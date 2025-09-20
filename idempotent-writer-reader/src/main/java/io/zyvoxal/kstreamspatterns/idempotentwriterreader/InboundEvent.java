package io.zyvoxal.kstreamspatterns.idempotentwriterreader;

public record InboundEvent(String eventId, String payload, String correlationId) {}
