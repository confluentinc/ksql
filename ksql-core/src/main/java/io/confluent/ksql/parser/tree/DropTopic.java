/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropTopic
    extends Statement {

  private final QualifiedName topicName;
  private final boolean exists;

  public DropTopic(QualifiedName tableName, boolean exists) {
    this(Optional.empty(), tableName, exists);
  }

  public DropTopic(NodeLocation location, QualifiedName tableName, boolean exists) {
    this(Optional.of(location), tableName, exists);
  }

  private DropTopic(Optional<NodeLocation> location, QualifiedName topicName, boolean exists) {
    super(location);
    this.topicName = topicName;
    this.exists = exists;
  }

  public QualifiedName getTopicName() {
    return topicName;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropTopic(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, exists);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropTopic o = (DropTopic) obj;
    return Objects.equals(topicName, o.topicName)
           && (exists == o.exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("topicName", topicName)
        .add("exists", exists)
        .toString();
  }
}
