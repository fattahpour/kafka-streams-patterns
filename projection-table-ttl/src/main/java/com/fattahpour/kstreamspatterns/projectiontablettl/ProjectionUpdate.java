package com.fattahpour.kstreamspatterns.projectiontablettl;

public record ProjectionUpdate(String id, long version, String payload, long eventTimestamp) {}
