package com.fattahpour.kstreamspatterns.eventcollaboration;

public record CollaborationEnvelope(SourceType sourceType, String value, String correlationId) {}
