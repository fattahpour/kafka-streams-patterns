package com.fattahpour.kstreamspatterns.wallclocktimers;

public record ScheduleCommand(String id, long dueAt, String payload, String correlationId) {}
