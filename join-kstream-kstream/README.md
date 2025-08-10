# join-kstream-kstream

Join two streams within a five-minute window.

```
left-join ----\
              +--> join --> joined
right-join ---/
```

## How to run

```bash
mvn -pl join-kstream-kstream -am clean package
java -jar join-kstream-kstream/target/join-kstream-kstream-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=join-kstream-kstream \
  -Dleft.topic=left-join \
  -Dright.topic=right-join \
  -Doutput.topic=joined
```

Produce matching records on `left-join` and `right-join` and observe joined values on `joined`.
