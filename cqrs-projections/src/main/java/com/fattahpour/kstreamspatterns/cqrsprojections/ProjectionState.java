package com.fattahpour.kstreamspatterns.cqrsprojections;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class ProjectionState {
  private String aggregateId;
  private String payload;
  private long version;

  public ProjectionState() {}

  public ProjectionState(String aggregateId, String payload, long version) {
    this.aggregateId = aggregateId;
    this.payload = payload;
    this.version = version;
  }

  public String aggregateId() {
    return aggregateId;
  }

  public String payload() {
    return payload;
  }

  public long version() {
    return version;
  }

  public void setAggregateId(String aggregateId) {
    this.aggregateId = aggregateId;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public void setVersion(long version) {
    this.version = version;
  }
}
