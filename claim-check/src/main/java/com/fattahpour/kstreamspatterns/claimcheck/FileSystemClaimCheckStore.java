package com.fattahpour.kstreamspatterns.claimcheck;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileSystemClaimCheckStore implements ClaimCheckStore {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemClaimCheckStore.class);
  private final Path baseDir;

  public FileSystemClaimCheckStore(Path baseDir) {
    this.baseDir = baseDir;
  }

  public static FileSystemClaimCheckStore defaultStore() {
    return new FileSystemClaimCheckStore(Path.of(System.getProperty("claim.check.store", "/tmp/blobstore")));
  }

  @Override
  public URI put(String id, byte[] payload) {
    try {
      Files.createDirectories(baseDir);
      String fileName = id + "-" + UUID.randomUUID();
      Path target = baseDir.resolve(fileName);
      Files.write(target, payload, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
      LOG.debug("Stored payload {} at {}", id, target);
      return target.toUri();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to store payload for id " + id, e);
    }
  }

  @Override
  public Optional<byte[]> get(URI uri) {
    try {
      Path path = Path.of(uri);
      if (!Files.exists(path)) {
        LOG.warn("Payload missing for URI {}", uri);
        return Optional.empty();
      }
      return Optional.of(Files.readAllBytes(path));
    } catch (IOException e) {
      LOG.error("Failed to read payload {}", uri, e);
      return Optional.empty();
    }
  }

  @Override
  public void delete(URI uri) {
    try {
      Files.deleteIfExists(Path.of(uri));
    } catch (IOException e) {
      LOG.warn("Failed to delete payload {}", uri, e);
    }
  }
}
