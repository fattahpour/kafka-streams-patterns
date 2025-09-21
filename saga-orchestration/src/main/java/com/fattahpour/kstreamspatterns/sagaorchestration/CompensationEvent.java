package com.fattahpour.kstreamspatterns.sagaorchestration;

public record CompensationEvent(String orderId, String type) {}
