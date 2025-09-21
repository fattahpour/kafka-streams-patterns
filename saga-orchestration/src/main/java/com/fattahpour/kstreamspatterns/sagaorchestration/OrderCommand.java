package com.fattahpour.kstreamspatterns.sagaorchestration;

public record OrderCommand(String orderId, boolean failPayment, String payload) {}
