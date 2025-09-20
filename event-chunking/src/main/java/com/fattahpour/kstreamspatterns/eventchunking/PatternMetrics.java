package com.fattahpour.kstreamspatterns.eventchunking;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong chunked = new AtomicLong();
  private final AtomicLong reassembled = new AtomicLong();
  private final AtomicLong duplicates = new AtomicLong();
  private final AtomicLong timeouts = new AtomicLong();

  public void markChunked() {
    chunked.incrementAndGet();
  }

  public void markReassembled() {
    reassembled.incrementAndGet();
  }

  public void markDuplicate() {
    duplicates.incrementAndGet();
  }

  public void markTimedOut() {
    timeouts.incrementAndGet();
  }

  public long chunked() {
    return chunked.get();
  }

  public long reassembled() {
    return reassembled.get();
  }

  public long duplicates() {
    return duplicates.get();
  }

  public long timeouts() {
    return timeouts.get();
  }
}
