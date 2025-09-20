package io.zyvoxal.kstreamspatterns.idempotentwriterreader;

import java.util.concurrent.atomic.AtomicLong;

public final class PatternMetrics {
  private final AtomicLong writerProcessed = new AtomicLong();
  private final AtomicLong writerEmitted = new AtomicLong();
  private final AtomicLong readerEmitted = new AtomicLong();
  private final AtomicLong dlq = new AtomicLong();

  public void markWriterProcessed() {
    writerProcessed.incrementAndGet();
  }

  public void markWriterEmitted() {
    writerEmitted.incrementAndGet();
  }

  public void markReaderEmitted() {
    readerEmitted.incrementAndGet();
  }

  public void markDlq() {
    dlq.incrementAndGet();
  }

  public long writerProcessed() {
    return writerProcessed.get();
  }

  public long writerEmitted() {
    return writerEmitted.get();
  }

  public long readerEmitted() {
    return readerEmitted.get();
  }

  public long dlq() {
    return dlq.get();
  }
}
