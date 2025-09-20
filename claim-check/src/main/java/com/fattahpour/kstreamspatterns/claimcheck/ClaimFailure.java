package com.fattahpour.kstreamspatterns.claimcheck;

public record ClaimFailure(String id, String reason, String correlationId) {}
