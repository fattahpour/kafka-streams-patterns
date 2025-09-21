package com.fattahpour.kstreamspatterns.cqrsprojections;

public record CommandError(String aggregateId, String reason) {}
