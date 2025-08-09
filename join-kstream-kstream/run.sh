#!/bin/bash
mvn -pl join-kstream-kstream -am package
java -jar join-kstream-kstream/target/join-kstream-kstream-1.0.0-SNAPSHOT.jar "$@"
