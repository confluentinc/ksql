/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Isolation
    extends TransactionMode {

  public enum Level {
    SERIALIZABLE("SERIALIZABLE"),
    REPEATABLE_READ("REPEATABLE READ"),
    READ_COMMITTED("READ COMMITTED"),
    READ_UNCOMMITTED("READ UNCOMMITTED");

    private final String text;

    Level(String text) {
      this.text = requireNonNull(text, "text is null");
    }

    public String getText() {
      return text;
    }
  }

  private final Level level;

  public Isolation(Level level) {
    this(Optional.empty(), level);
  }

  public Isolation(NodeLocation location, Level level) {
    this(Optional.of(location), level);
  }

  private Isolation(Optional<NodeLocation> location, Level level) {
    super(location);
    this.level = requireNonNull(level, "level is null");
  }

  public Level getLevel() {
    return level;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIsolationLevel(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(level);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Isolation other = (Isolation) obj;
    return this.level == other.level;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("level", level)
        .toString();
  }
}
