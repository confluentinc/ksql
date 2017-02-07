/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Explain
    extends Statement {

  private final Statement statement;
  private final boolean analyze;
  private final List<ExplainOption> options;

  public Explain(Statement statement, boolean analyze, List<ExplainOption> options) {
    this(Optional.empty(), analyze, statement, options);
  }

  public Explain(NodeLocation location, boolean analyze, Statement statement,
                 List<ExplainOption> options) {
    this(Optional.of(location), analyze, statement, options);
  }

  private Explain(Optional<NodeLocation> location, boolean analyze, Statement statement,
                  List<ExplainOption> options) {
    super(location);
    this.statement = requireNonNull(statement, "statement is null");
    this.analyze = analyze;
    if (options == null) {
      this.options = ImmutableList.of();
    } else {
      this.options = ImmutableList.copyOf(options);
    }
  }

  public Statement getStatement() {
    return statement;
  }

  public boolean isAnalyze() {
    return analyze;
  }

  public List<ExplainOption> getOptions() {
    return options;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, options, analyze);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Explain o = (Explain) obj;
    return Objects.equals(statement, o.statement) &&
           Objects.equals(options, o.options) &&
           Objects.equals(analyze, o.analyze);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
        .add("options", options)
        .add("analyze", analyze)
        .toString();
  }
}
