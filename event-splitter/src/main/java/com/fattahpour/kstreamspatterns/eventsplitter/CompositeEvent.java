package com.fattahpour.kstreamspatterns.eventsplitter;

import java.util.List;

public record CompositeEvent(String id, String correlationId, List<String> fragments) {}
