package com.fattahpour.kstreamspatterns.sagaorchestration;

public record OrderError(String orderId, String reason) {}
