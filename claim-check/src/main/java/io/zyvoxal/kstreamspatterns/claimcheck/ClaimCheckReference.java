package io.zyvoxal.kstreamspatterns.claimcheck;

import java.net.URI;

public record ClaimCheckReference(String id, URI uri, String correlationId) {}
