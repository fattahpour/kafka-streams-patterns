package io.zyvoxal.kstreamspatterns.claimcheck;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong references = new AtomicLong();
  private final AtomicLong resolved = new AtomicLong();
  private final AtomicLong fallbacks = new AtomicLong();
  private final AtomicLong dlq = new AtomicLong();

  public void markProcessed() {
    processed.incrementAndGet();
  }

  public void markReference() {
    references.incrementAndGet();
  }

  public void markResolved() {
    resolved.incrementAndGet();
  }

  public void markFallback() {
    fallbacks.incrementAndGet();
  }

  public void markDlq() {
    dlq.incrementAndGet();
  }

  public long processed() {
    return processed.get();
  }

  public long references() {
    return references.get();
  }

  public long resolved() {
    return resolved.get();
  }

  public long fallbacks() {
    return fallbacks.get();
  }

  public long dlq() {
    return dlq.get();
  }
}
