#!/bin/bash
mvn -q -pl retry-dlq -am clean package
java -jar retry-dlq/target/retry-dlq-1.0.0-SNAPSHOT.jar
