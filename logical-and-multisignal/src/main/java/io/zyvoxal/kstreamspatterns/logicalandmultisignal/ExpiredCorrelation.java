package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import java.util.Set;

public record ExpiredCorrelation(String correlationKey, String correlationId, Set<SignalType> missingSignals) {}
