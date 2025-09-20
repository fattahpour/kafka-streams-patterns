package com.fattahpour.kstreamspatterns.claimcheck;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryClaimCheckStore implements ClaimCheckStore {
  private final Map<URI, byte[]> store = new ConcurrentHashMap<>();

  @Override
  public URI put(String id, byte[] payload) {
    URI uri = URI.create("memory://" + id + "/" + store.size());
    store.put(uri, payload);
    return uri;
  }

  @Override
  public Optional<byte[]> get(URI uri) {
    return Optional.ofNullable(store.get(uri));
  }
}
