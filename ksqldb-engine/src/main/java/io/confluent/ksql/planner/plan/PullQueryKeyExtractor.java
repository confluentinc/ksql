package io.confluent.ksql.planner.plan;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physical.pull.operators.WhereInfo;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;

public class PullQueryKeyExtractor {

  public PullQueryKeyExtractor() {

  }

  public static List<KeyConstraint> extractKeyConstraints(
      final Expression expression,
      final LogicalSchema logicalSchema,
      final boolean windowed,
      final MetaStore metaStore,
      final KsqlConfig config) {
    Expression rewritten = PullQueryRewriter.rewrite(expression);
    List<Expression> disjuncts = LogicRewriter.extractDisjuncts(rewritten);
    // We need to be able to extract KeyConstraints for all disjuncts, otherwise
    ImmutableList.Builder<KeyConstraint> constraints = ImmutableList.builder();
    for (Expression disjunct : disjuncts) {
      WhereInfo whereInfo = WhereInfo.extractWhereInfo(disjunct, logicalSchema, windowed, metaStore,
          config);
      if (!whereInfo.getKeyBound().isPresent()) {
        constraints.add(new UnboundKeyConstraint());
      } else {
        constraints.add(new KeyEqualityConstraint(whereInfo.getKeyBound().get()));
      }
    }
    return constraints.build();
  }

  public interface KeyConstraint {
    GenericKey getKey();
    ConstraintType getConstraintType();
    ConstraintOperator getConstraintOperator();
  }

  public enum ConstraintType {
    NONE,
    EQUALITY,
    RANGE
  }

  public enum ConstraintOperator {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
  }

  public static class UnboundKeyConstraint implements KeyConstraint {

    @Override
    public GenericKey getKey() {
      return null;
    }

    @Override
    public ConstraintType getConstraintType() {
      return ConstraintType.NONE;
    }

    @Override
    public ConstraintOperator getConstraintOperator() {
      return null;
    }
  }

  public static class KeyEqualityConstraint implements KeyConstraint {

    private final GenericKey key;

    public KeyEqualityConstraint(final GenericKey key) {
      this.key = key;
    }

    @Override
    public GenericKey getKey() {
      return key;
    }

    @Override
    public ConstraintType getConstraintType() {
      return ConstraintType.EQUALITY;
    }

    @Override
    public ConstraintOperator getConstraintOperator() {
      return ConstraintOperator.EQUAL;
    }
  }
}
