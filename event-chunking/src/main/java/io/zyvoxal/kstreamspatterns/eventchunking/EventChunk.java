package io.zyvoxal.kstreamspatterns.eventchunking;

public record EventChunk(String id, int sequence, int totalChunks, String fragment, String correlationId) {}
