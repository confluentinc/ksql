package io.confluent.ksql.rest.server.computation;

public interface WriteOnceStore<V> {
  V readMaybeWrite(V value);
}
