package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

public record SignalEnvelope(String correlationKey, String signalType, String payload, String correlationId) {}
