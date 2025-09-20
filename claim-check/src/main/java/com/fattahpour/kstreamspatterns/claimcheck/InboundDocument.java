package com.fattahpour.kstreamspatterns.claimcheck;

public record InboundDocument(String id, String payload, String correlationId, String contentType) {}
