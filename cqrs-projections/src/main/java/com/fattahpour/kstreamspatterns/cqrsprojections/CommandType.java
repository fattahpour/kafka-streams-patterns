package com.fattahpour.kstreamspatterns.cqrsprojections;

enum CommandType {
  CREATE,
  UPDATE,
  DELETE;

  static CommandType fromString(String value) {
    if (value == null) {
      throw new IllegalArgumentException("type is null");
    }
    return CommandType.valueOf(value.toUpperCase());
  }
}
