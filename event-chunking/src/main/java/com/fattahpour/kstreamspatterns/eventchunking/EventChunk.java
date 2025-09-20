package com.fattahpour.kstreamspatterns.eventchunking;

public record EventChunk(String id, int sequence, int totalChunks, String fragment, String correlationId) {}
