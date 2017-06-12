/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropStream
    extends Statement {

  private final QualifiedName streamName;
  private final boolean exists;

  public DropStream(QualifiedName tableName, boolean exists) {
    this(Optional.empty(), tableName, exists);
  }

  public DropStream(NodeLocation location, QualifiedName tableName, boolean exists) {
    this(Optional.of(location), tableName, exists);
  }

  private DropStream(Optional<NodeLocation> location, QualifiedName streamName, boolean exists) {
    super(location);
    this.streamName = streamName;
    this.exists = exists;
  }

  public QualifiedName getStreamName() {
    return streamName;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropStream(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, exists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropStream o = (DropStream) obj;
    return Objects.equals(streamName, o.streamName)
           && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", streamName)
        .add("exists", exists)
        .toString();
  }
}
