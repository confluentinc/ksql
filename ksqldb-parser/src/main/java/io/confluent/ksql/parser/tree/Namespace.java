package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class Namespace {

  public static Namespace PRIMARY_KEY = new Namespace("PRIMARY_KEY", Optional.empty());
  public static Namespace KEY = new Namespace("KEY", Optional.empty());
  public static Namespace VALUE = new Namespace("VALUE", Optional.empty());
  public static Namespace HEADERS = new Namespace("HEADERS", Optional.empty());
  private final String type;
  private final Optional<String> key;

  Namespace(final String type, final Optional<String> key) {
    this.type = type;
    this.key = key;
  }

  public static Namespace createHeaderNamespace(final Optional<String> key) {
    return new Namespace("HEADERS", key);
  }

  public String getType() {
    return type;
  }

  public Optional<String> getKey() {
    return key;
  }

  public boolean isKey() {
    return type == "PRIMARY_KEY" || type == "KEY";
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, key);
  }

  @Override
  public String toString() {
    return type;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Namespace o = (Namespace) obj;
    return Objects.equals(this.type, o.type)
        && Objects.equals(this.key, o.key);
  }
}
