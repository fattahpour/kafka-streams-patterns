package io.zyvoxal.kstreamspatterns.eventchunking;

public record ChunkMergeResult(ReassembledEvent reassembled, ChunkTimeout timeout) {}
