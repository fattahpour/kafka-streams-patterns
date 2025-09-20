package io.zyvoxal.kstreamspatterns.claimcheck;

public record ClaimFailure(String id, String reason, String correlationId) {}
