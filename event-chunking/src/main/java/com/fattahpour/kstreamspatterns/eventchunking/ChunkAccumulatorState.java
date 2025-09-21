package com.fattahpour.kstreamspatterns.eventchunking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class ChunkAccumulatorState {
  private String id;
  private int totalChunks;
  private Map<Integer, String> fragments = new HashMap<>();
  private long createdAt;
  private long updatedAt;
  private String correlationId;

  public ChunkAccumulatorState() {}

  public ChunkAccumulatorState(String id, int totalChunks, long createdAt, String correlationId) {
    this.id = id;
    this.totalChunks = totalChunks;
    this.createdAt = createdAt;
    this.updatedAt = createdAt;
    this.correlationId = correlationId;
  }

  public String id() {
    return id;
  }

  public int totalChunks() {
    return totalChunks;
  }

  public Map<Integer, String> fragments() {
    return fragments;
  }

  public long createdAt() {
    return createdAt;
  }

  public long updatedAt() {
    return updatedAt;
  }

  public String correlationId() {
    return correlationId;
  }

  public void addFragment(int sequence, String fragment, long timestamp) {
    fragments.put(sequence, fragment);
    this.updatedAt = timestamp;
  }

  public boolean hasFragment(int sequence) {
    return fragments.containsKey(sequence);
  }

  @JsonIgnore
  public boolean isComplete() {
    return fragments.size() == totalChunks;
  }

  public String join() {
    StringBuilder builder = new StringBuilder();
    fragments.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> builder.append(entry.getValue()));
    return builder.toString();
  }

  public Set<Integer> missingSequences() {
    Set<Integer> missing = new TreeSet<>();
    for (int i = 0; i < totalChunks; i++) {
      if (!fragments.containsKey(i)) {
        missing.add(i);
      }
    }
    return missing;
  }
}
