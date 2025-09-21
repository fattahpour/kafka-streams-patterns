package com.fattahpour.kstreamspatterns.eventsplitter;

public record ChildEvent(String childId, String parentId, int index, String payload, String correlationId) {}
