package com.fattahpour.kstreamspatterns.logicalandmultisignal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class CorrelationState {
  private String correlationKey;
  private String correlationId;
  private Set<String> receivedTypes = new HashSet<>();
  private Map<String, String> payloads = new HashMap<>();
  private long firstTimestamp;
  private long updatedAt;

  public CorrelationState() {}

  public CorrelationState(String correlationKey, String correlationId, long timestamp) {
    this.correlationKey = correlationKey;
    this.correlationId = correlationId;
    this.firstTimestamp = timestamp;
    this.updatedAt = timestamp;
  }

  public void add(SignalType type, String payload, String correlationId, long timestamp) {
    receivedTypes.add(type.name());
    payloads.putIfAbsent(type.name(), payload);
    if (this.correlationId == null) {
      this.correlationId = correlationId;
    }
    this.updatedAt = timestamp;
  }

  @JsonIgnore
  public boolean isComplete() {
    for (SignalType type : SignalType.values()) {
      if (!receivedTypes.contains(type.name())) {
        return false;
      }
    }
    return true;
  }

  public Set<SignalType> missingSignals() {
    EnumSet<SignalType> missing = EnumSet.noneOf(SignalType.class);
    for (SignalType type : SignalType.values()) {
      if (!receivedTypes.contains(type.name())) {
        missing.add(type);
      }
    }
    return missing;
  }

  public Map<String, String> payloads() {
    return payloads;
  }

  public String correlationKey() {
    return correlationKey;
  }

  public String correlationId() {
    return correlationId;
  }

  public long firstTimestamp() {
    return firstTimestamp;
  }

  public long updatedAt() {
    return updatedAt;
  }

  public void setCorrelationKey(String correlationKey) {
    this.correlationKey = correlationKey;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public void setFirstTimestamp(long firstTimestamp) {
    this.firstTimestamp = firstTimestamp;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public Set<String> getReceivedTypes() {
    return receivedTypes;
  }

  public void setReceivedTypes(Set<String> receivedTypes) {
    this.receivedTypes = receivedTypes;
  }

  public Map<String, String> getPayloads() {
    return payloads;
  }

  public void setPayloads(Map<String, String> payloads) {
    this.payloads = payloads;
  }
}
