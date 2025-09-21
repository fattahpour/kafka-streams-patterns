package com.fattahpour.kstreamspatterns.wallclocktimers;

public record ScheduledTimer(String id, long dueAt, String payload, String correlationId, long createdAt) {}
