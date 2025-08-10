package com.fattahpour.kstreamspatterns.common;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public final class HeadersUtil {
  private HeadersUtil() {}

  public static String getAsString(Headers headers, String key) {
    Header h = headers.lastHeader(key);
    return h == null ? null : new String(h.value());
  }
}
