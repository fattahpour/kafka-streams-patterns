package com.fattahpour.kstreamspatterns.eventcollaboration;

public record CollaboratedEvent(
    String id, String alphaValue, String betaValue, String correlationId, long eventTime) {}
