package com.fattahpour.kstreamspatterns.wallclocktimers;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong scheduled = new AtomicLong();
  private final AtomicLong fired = new AtomicLong();
  private final AtomicLong invalid = new AtomicLong();

  public void markScheduled() {
    scheduled.incrementAndGet();
  }

  public void markFired() {
    fired.incrementAndGet();
  }

  public void markInvalid() {
    invalid.incrementAndGet();
  }

  public long scheduled() {
    return scheduled.get();
  }

  public long fired() {
    return fired.get();
  }

  public long invalid() {
    return invalid.get();
  }
}
