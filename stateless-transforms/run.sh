#!/bin/bash
mvn -pl stateless-transforms -am package
java -jar stateless-transforms/target/stateless-transforms-1.0.0-SNAPSHOT.jar "$@"
