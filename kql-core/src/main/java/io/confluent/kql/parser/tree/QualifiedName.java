/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public class QualifiedName {

  private final List<String> parts;

  public static QualifiedName of(String first, String... rest) {
    requireNonNull(first, "first is null");
    return of(ImmutableList.copyOf(Lists.asList(first, rest)));
  }

  public static QualifiedName of(String name) {
    requireNonNull(name, "name is null");
    return of(ImmutableList.of(name));
  }

  public static QualifiedName of(Iterable<String> originalParts) {
    requireNonNull(originalParts, "originalParts is null");
    checkArgument(!isEmpty(originalParts), "originalParts is empty");
    List<String>
        parts =
        ImmutableList.copyOf(transform(originalParts, String::toUpperCase));

    return new QualifiedName(parts);
  }

  private QualifiedName(List<String> parts) {
    this.parts = parts;
  }

  public List<String> getParts() {
    return parts;
  }

  @Override
  public String toString() {
    return Joiner.on('.').join(parts).toUpperCase();
  }

  /**
   * For an identifier of the form "a.b.c.d", returns "a.b.c"
   * For an identifier of the form "a", returns absent
   */
  public Optional<QualifiedName> getPrefix() {
    if (parts.size() == 1) {
      return Optional.empty();
    }

    List<String> subList = parts.subList(0, parts.size() - 1);
    return Optional.of(new QualifiedName(subList));
  }

  public boolean hasSuffix(QualifiedName suffix) {
    if (parts.size() < suffix.getParts().size()) {
      return false;
    }

    int start = parts.size() - suffix.getParts().size();

    return parts.subList(start, parts.size()).equals(suffix.getParts());
  }

  public String getSuffix() {
    return Iterables.getLast(parts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return parts.equals(((QualifiedName) o).parts);
  }

  @Override
  public int hashCode() {
    return parts.hashCode();
  }
}
