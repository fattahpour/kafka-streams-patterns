package com.fattahpour.kstreamspatterns.eventchunking;

import java.util.Set;

public record ChunkTimeout(String id, Set<Integer> missingSequences, String correlationId) {}
