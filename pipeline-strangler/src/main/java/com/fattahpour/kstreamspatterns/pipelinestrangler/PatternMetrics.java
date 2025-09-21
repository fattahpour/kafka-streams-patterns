package com.fattahpour.kstreamspatterns.pipelinestrangler;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong ingress = new AtomicLong();
  private final AtomicLong legacyRouted = new AtomicLong();
  private final AtomicLong modernRouted = new AtomicLong();

  public void markIngress() {
    ingress.incrementAndGet();
  }

  public void markLegacy() {
    legacyRouted.incrementAndGet();
  }

  public void markModern() {
    modernRouted.incrementAndGet();
  }

  public long ingress() {
    return ingress.get();
  }

  public long legacyRouted() {
    return legacyRouted.get();
  }

  public long modernRouted() {
    return modernRouted.get();
  }
}
