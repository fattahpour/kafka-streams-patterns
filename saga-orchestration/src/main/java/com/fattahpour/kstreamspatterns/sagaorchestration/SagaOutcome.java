package com.fattahpour.kstreamspatterns.sagaorchestration;

import java.util.List;

public record SagaOutcome(List<SagaEvent> events, CompensationEvent compensation, OrderError error) {}
