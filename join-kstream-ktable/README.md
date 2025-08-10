# join-kstream-ktable

Join a stream with a table using an inner join.

```
stream-join ---\
             +--> join --> joined
table-join --/
```

## How to run

```bash
mvn -pl join-kstream-ktable -am clean package
java -jar join-kstream-ktable/target/join-kstream-ktable-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=join-kstream-ktable \
  -Dstream.topic=stream \
  -Dtable.topic=table \
  -Doutput.topic=joined
```

Produce records on `stream` and `table` and observe joined values on `joined`.
