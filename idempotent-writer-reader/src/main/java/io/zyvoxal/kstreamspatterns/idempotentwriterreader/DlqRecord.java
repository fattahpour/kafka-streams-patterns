package io.zyvoxal.kstreamspatterns.idempotentwriterreader;

public record DlqRecord(String eventId, String reason, String correlationId) {}
