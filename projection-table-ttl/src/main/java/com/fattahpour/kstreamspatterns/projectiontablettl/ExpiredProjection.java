package com.fattahpour.kstreamspatterns.projectiontablettl;

public record ExpiredProjection(String id, long version, long expiredAt) {}
