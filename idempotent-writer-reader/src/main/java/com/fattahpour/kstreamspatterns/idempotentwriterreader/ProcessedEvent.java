package com.fattahpour.kstreamspatterns.idempotentwriterreader;

public record ProcessedEvent(String eventId, String payload, long deliveryTimestamp, String correlationId) {}
