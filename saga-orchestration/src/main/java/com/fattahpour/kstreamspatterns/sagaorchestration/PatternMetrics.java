package com.fattahpour.kstreamspatterns.sagaorchestration;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong started = new AtomicLong();
  private final AtomicLong completed = new AtomicLong();
  private final AtomicLong compensated = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

  public void markStarted() {
    started.incrementAndGet();
  }

  public void markCompleted() {
    completed.incrementAndGet();
  }

  public void markCompensated() {
    compensated.incrementAndGet();
  }

  public void markRejected() {
    rejected.incrementAndGet();
  }

  public long started() {
    return started.get();
  }

  public long completed() {
    return completed.get();
  }

  public long compensated() {
    return compensated.get();
  }

  public long rejected() {
    return rejected.get();
  }
}
