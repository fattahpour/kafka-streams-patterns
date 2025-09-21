package com.fattahpour.kstreamspatterns.contentfilter;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong accepted = new AtomicLong();
  private final AtomicLong dropped = new AtomicLong();

  public void markProcessed() {
    processed.incrementAndGet();
  }

  public void markAccepted() {
    accepted.incrementAndGet();
  }

  public void markDropped() {
    dropped.incrementAndGet();
  }

  public long processed() {
    return processed.get();
  }

  public long accepted() {
    return accepted.get();
  }

  public long dropped() {
    return dropped.get();
  }
}
