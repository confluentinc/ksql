/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionCall
    extends Expression {

  private final QualifiedName name;
  private final Optional<Window> window;
  private final boolean distinct;
  private final List<Expression> arguments;

  public FunctionCall(QualifiedName name, List<Expression> arguments) {
    this(Optional.empty(), name, Optional.<Window>empty(), false, arguments);
  }

  public FunctionCall(NodeLocation location, QualifiedName name, List<Expression> arguments) {
    this(Optional.of(location), name, Optional.<Window>empty(), false, arguments);
  }

  public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments) {
    this(Optional.empty(), name, Optional.<Window>empty(), distinct, arguments);
  }

  public FunctionCall(NodeLocation location, QualifiedName name, boolean distinct,
                      List<Expression> arguments) {
    this(Optional.of(location), name, Optional.<Window>empty(), distinct, arguments);
  }

  public FunctionCall(QualifiedName name, Optional<Window> window, boolean distinct,
                      List<Expression> arguments) {
    this(Optional.empty(), name, window, distinct, arguments);
  }

  public FunctionCall(NodeLocation location, QualifiedName name, Optional<Window> window,
                      boolean distinct, List<Expression> arguments) {
    this(Optional.of(location), name, window, distinct, arguments);
  }

  private FunctionCall(Optional<NodeLocation> location, QualifiedName name, Optional<Window> window,
                       boolean distinct, List<Expression> arguments) {
    super(location);
    requireNonNull(name, "name is null");
    requireNonNull(window, "window is null");
    requireNonNull(arguments, "arguments is null");

    this.name = name;
    this.window = window;
    this.distinct = distinct;
    this.arguments = arguments;
  }

  public QualifiedName getName() {
    return name;
  }

  public Optional<Window> getWindow() {
    return window;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFunctionCall(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    FunctionCall o = (FunctionCall) obj;
    return Objects.equals(name, o.name) &&
           Objects.equals(window, o.window) &&
           Objects.equals(distinct, o.distinct) &&
           Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, distinct, window, arguments);
  }
}
