package io.zyvoxal.kstreamspatterns.logicalandmultisignal;

enum SignalType {
  A,
  B,
  C;

  static SignalType fromString(String value) {
    return SignalType.valueOf(value.toUpperCase());
  }
}
