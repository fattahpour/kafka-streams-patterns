package com.fattahpour.kstreamspatterns.eventsplitter;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong split = new AtomicLong();
  private final AtomicLong fragmentsEmitted = new AtomicLong();
  private final AtomicLong invalid = new AtomicLong();

  public void markSplit() {
    split.incrementAndGet();
  }

  public void markFragments(int count) {
    fragmentsEmitted.addAndGet(count);
  }

  public void markInvalid() {
    invalid.incrementAndGet();
  }

  public long split() {
    return split.get();
  }

  public long fragmentsEmitted() {
    return fragmentsEmitted.get();
  }

  public long invalid() {
    return invalid.get();
  }
}
