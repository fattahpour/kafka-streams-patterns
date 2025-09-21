package com.fattahpour.kstreamspatterns.pipelinestrangler;

enum RoutingMode {
  LEGACY,
  MODERN,
  DUAL;

  static RoutingMode from(String raw) {
    if (raw == null) {
      return DUAL;
    }
    return switch (raw.trim().toLowerCase()) {
      case "legacy" -> LEGACY;
      case "modern" -> MODERN;
      case "dual" -> DUAL;
      default -> DUAL;
    };
  }
}
