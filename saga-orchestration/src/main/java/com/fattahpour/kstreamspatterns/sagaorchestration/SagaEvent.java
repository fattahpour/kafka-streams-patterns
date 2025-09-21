package com.fattahpour.kstreamspatterns.sagaorchestration;

public record SagaEvent(String orderId, String type, String payload) {}
