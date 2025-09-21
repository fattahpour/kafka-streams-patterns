package com.fattahpour.kstreamspatterns.eventcollaboration;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong alphaReceived = new AtomicLong();
  private final AtomicLong betaReceived = new AtomicLong();
  private final AtomicLong joined = new AtomicLong();
  private final AtomicLong late = new AtomicLong();

  public void markAlpha() {
    alphaReceived.incrementAndGet();
  }

  public void markBeta() {
    betaReceived.incrementAndGet();
  }

  public void markJoined() {
    joined.incrementAndGet();
  }

  public void markLate() {
    late.incrementAndGet();
  }

  public long alphaReceived() {
    return alphaReceived.get();
  }

  public long betaReceived() {
    return betaReceived.get();
  }

  public long joined() {
    return joined.get();
  }

  public long late() {
    return late.get();
  }
}
