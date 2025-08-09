#!/bin/bash
mvn -pl join-kstream-ktable -am package
java -jar join-kstream-ktable/target/join-kstream-ktable-1.0.0-SNAPSHOT.jar "$@"
