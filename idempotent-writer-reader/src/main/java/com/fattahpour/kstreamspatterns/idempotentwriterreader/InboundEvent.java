package com.fattahpour.kstreamspatterns.idempotentwriterreader;

public record InboundEvent(String eventId, String payload, String correlationId) {}
