package com.fattahpour.kstreamspatterns.idempotentwriterreader;

public record DeduplicatedEvent(String eventId, String payload, String correlationId) {}
