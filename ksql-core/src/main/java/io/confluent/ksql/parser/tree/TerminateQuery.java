package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TerminateQuery extends Statement {

  private final QualifiedName queryId;

  public TerminateQuery(QualifiedName queryId) {
    this(Optional.empty(), queryId);
  }

  public TerminateQuery(NodeLocation location, QualifiedName queryId) {
    this(Optional.of(location), queryId);
  }

  private TerminateQuery(Optional<NodeLocation> location, QualifiedName queryId) {
    super(location);
    this.queryId = requireNonNull(queryId, "table is null");
  }

  public QualifiedName getQueryId() {
    return queryId;
  }

  @Override
  public int hashCode() {
    return Objects.hash("TerminateQuery");
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }
}
