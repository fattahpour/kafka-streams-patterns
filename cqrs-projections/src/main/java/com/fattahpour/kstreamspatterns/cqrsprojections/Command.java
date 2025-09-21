package com.fattahpour.kstreamspatterns.cqrsprojections;

public record Command(String aggregateId, String type, String payload) {}
