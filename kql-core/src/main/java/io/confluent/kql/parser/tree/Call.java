/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Call
    extends Statement {

  private final QualifiedName name;
  private final List<CallArgument> arguments;

  public Call(QualifiedName name, List<CallArgument> arguments) {
    this(Optional.empty(), name, arguments);
  }

  public Call(NodeLocation location, QualifiedName name, List<CallArgument> arguments) {
    this(Optional.of(location), name, arguments);
  }

  public Call(Optional<NodeLocation> location, QualifiedName name, List<CallArgument> arguments) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
  }

  public QualifiedName getName() {
    return name;
  }

  public List<CallArgument> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCall(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Call o = (Call) obj;
    return Objects.equals(name, o.name) &&
           Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("arguments", arguments)
        .toString();
  }
}
