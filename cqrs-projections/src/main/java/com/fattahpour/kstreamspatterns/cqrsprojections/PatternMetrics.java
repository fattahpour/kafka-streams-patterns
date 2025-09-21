package com.fattahpour.kstreamspatterns.cqrsprojections;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong accepted = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

  public void markAccepted() {
    accepted.incrementAndGet();
  }

  public void markRejected() {
    rejected.incrementAndGet();
  }

  public long accepted() {
    return accepted.get();
  }

  public long rejected() {
    return rejected.get();
  }
}
