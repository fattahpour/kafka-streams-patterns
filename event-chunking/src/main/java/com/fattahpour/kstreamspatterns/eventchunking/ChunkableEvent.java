package com.fattahpour.kstreamspatterns.eventchunking;

public record ChunkableEvent(String id, String payload, String correlationId) {}
