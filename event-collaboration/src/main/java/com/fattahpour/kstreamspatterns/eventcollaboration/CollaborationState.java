package com.fattahpour.kstreamspatterns.eventcollaboration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class CollaborationState {
  private String correlationId;
  private String alphaValue;
  private Long alphaTimestamp;
  private String betaValue;
  private Long betaTimestamp;

  public CollaborationState() {}

  public String correlationId() {
    return correlationId;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public String alphaValue() {
    return alphaValue;
  }

  public void setAlphaValue(String alphaValue) {
    this.alphaValue = alphaValue;
  }

  public Long alphaTimestamp() {
    return alphaTimestamp;
  }

  public void setAlphaTimestamp(Long alphaTimestamp) {
    this.alphaTimestamp = alphaTimestamp;
  }

  public String betaValue() {
    return betaValue;
  }

  public void setBetaValue(String betaValue) {
    this.betaValue = betaValue;
  }

  public Long betaTimestamp() {
    return betaTimestamp;
  }

  public void setBetaTimestamp(Long betaTimestamp) {
    this.betaTimestamp = betaTimestamp;
  }
}
