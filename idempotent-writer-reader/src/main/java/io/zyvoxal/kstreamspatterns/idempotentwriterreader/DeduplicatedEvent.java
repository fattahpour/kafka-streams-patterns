package io.zyvoxal.kstreamspatterns.idempotentwriterreader;

public record DeduplicatedEvent(String eventId, String payload, String correlationId) {}
