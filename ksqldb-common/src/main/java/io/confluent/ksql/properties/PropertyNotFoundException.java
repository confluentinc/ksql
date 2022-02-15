package io.confluent.ksql.properties;

public class PropertyNotFoundException extends IllegalArgumentException {
  PropertyNotFoundException(final String property) {
    super(String.format(
        "Not recognizable as ksql, streams, consumer, or producer property: '%s'",
        property
    ));
  }
}
