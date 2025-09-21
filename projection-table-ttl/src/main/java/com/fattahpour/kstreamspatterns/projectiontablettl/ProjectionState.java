package com.fattahpour.kstreamspatterns.projectiontablettl;

public record ProjectionState(long version, String payload, long updatedAt) {}
