package io.zyvoxal.kstreamspatterns.eventchunking;

import java.util.Set;

public record ChunkTimeout(String id, Set<Integer> missingSequences, String correlationId) {}
