package io.zyvoxal.kstreamspatterns.claimcheck;

public record ResolvedDocument(String id, String payload, boolean fallbackUsed, String correlationId) {}
