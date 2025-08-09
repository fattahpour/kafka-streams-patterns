#!/bin/bash
mvn -pl join-ktable-ktable -am package
java -jar join-ktable-ktable/target/join-ktable-ktable-1.0.0-SNAPSHOT.jar "$@"
