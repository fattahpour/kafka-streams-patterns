package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong completed = new AtomicLong();
  private final AtomicLong expired = new AtomicLong();

  public void markProcessed() {
    processed.incrementAndGet();
  }

  public void markCompleted() {
    completed.incrementAndGet();
  }

  public void markExpired() {
    expired.incrementAndGet();
  }

  public long processed() {
    return processed.get();
  }

  public long completed() {
    return completed.get();
  }

  public long expired() {
    return expired.get();
  }
}
