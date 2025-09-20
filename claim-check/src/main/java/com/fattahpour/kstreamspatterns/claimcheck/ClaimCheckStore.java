package com.fattahpour.kstreamspatterns.claimcheck;

import java.net.URI;
import java.util.Optional;

public interface ClaimCheckStore {
  URI put(String id, byte[] payload);

  Optional<byte[]> get(URI uri);

  default void delete(URI uri) {
    // Optional hook for implementations.
  }
}
