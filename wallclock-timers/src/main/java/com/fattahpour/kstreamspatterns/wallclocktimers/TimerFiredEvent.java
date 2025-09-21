package com.fattahpour.kstreamspatterns.wallclocktimers;

public record TimerFiredEvent(String id, long dueAt, long firedAt, String payload, String correlationId) {}
