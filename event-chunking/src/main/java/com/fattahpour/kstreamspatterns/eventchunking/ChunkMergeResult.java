package com.fattahpour.kstreamspatterns.eventchunking;

public record ChunkMergeResult(ReassembledEvent reassembled, ChunkTimeout timeout) {}
