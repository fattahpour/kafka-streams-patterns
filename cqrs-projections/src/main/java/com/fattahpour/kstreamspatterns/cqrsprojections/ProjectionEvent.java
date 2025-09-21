package com.fattahpour.kstreamspatterns.cqrsprojections;

public record ProjectionEvent(String aggregateId, String payload, long version, String action) {}
