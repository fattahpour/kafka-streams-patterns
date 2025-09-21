package com.fattahpour.kstreamspatterns.projectiontablettl;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong upserts = new AtomicLong();
  private final AtomicLong skipped = new AtomicLong();
  private final AtomicLong expired = new AtomicLong();

  public void markUpsert() {
    upserts.incrementAndGet();
  }

  public void markSkipped() {
    skipped.incrementAndGet();
  }

  public void markExpired() {
    expired.incrementAndGet();
  }

  public long upserts() {
    return upserts.get();
  }

  public long skipped() {
    return skipped.get();
  }

  public long expired() {
    return expired.get();
  }
}
