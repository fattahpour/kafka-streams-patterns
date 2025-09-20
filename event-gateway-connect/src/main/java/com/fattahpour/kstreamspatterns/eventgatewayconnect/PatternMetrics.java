package com.fattahpour.kstreamspatterns.eventgatewayconnect;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong success = new AtomicLong();
  private final AtomicLong retries = new AtomicLong();
  private final AtomicLong dlq = new AtomicLong();

  public void markProcessed() {
    processed.incrementAndGet();
  }

  public void markSuccess() {
    success.incrementAndGet();
  }

  public void markRetry() {
    retries.incrementAndGet();
  }

  public void markDlq() {
    dlq.incrementAndGet();
  }

  public long processed() {
    return processed.get();
  }

  public long success() {
    return success.get();
  }

  public long retries() {
    return retries.get();
  }

  public long dlq() {
    return dlq.get();
  }
}
