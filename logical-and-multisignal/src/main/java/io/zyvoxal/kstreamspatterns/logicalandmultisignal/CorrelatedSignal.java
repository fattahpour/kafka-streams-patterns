package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import java.util.Map;

public record CorrelatedSignal(String correlationKey, String correlationId, Map<SignalType, String> payloads, long completedAt) {}
