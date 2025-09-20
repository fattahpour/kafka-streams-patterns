package io.zyvoxal.kstreamspatterns.eventchunking;

import java.util.ArrayList;
import java.util.List;

final class PayloadChunker {
  private PayloadChunker() {}

  static List<EventChunk> split(ChunkableEvent event, int chunkSize) {
    List<EventChunk> chunks = new ArrayList<>();
    if (event.payload() == null) {
      chunks.add(new EventChunk(event.id(), 0, 1, "", event.correlationId()));
      return chunks;
    }
    String payload = event.payload();
    int total = (int) Math.ceil(payload.length() / (double) chunkSize);
    for (int i = 0; i < total; i++) {
      int start = i * chunkSize;
      int end = Math.min(payload.length(), start + chunkSize);
      String fragment = payload.substring(start, end);
      chunks.add(new EventChunk(event.id(), i, total, fragment, event.correlationId()));
    }
    return chunks;
  }
}
