package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.planner.Schema;
import org.apache.kafka.streams.kstream.Predicate;

public class SQLPredicate {

    final ComparisonExpression comparisonExpression;
    final Schema schema;

    public SQLPredicate(ComparisonExpression comparisonExpression, Schema schema) {
        this.comparisonExpression = comparisonExpression;
        this.schema = schema;
    }

    public Predicate getPredicate() {
        return new Predicate<String, GenericRow>() {
            @Override
            public boolean test(String key, GenericRow row) {
                QualifiedNameReference left = (QualifiedNameReference) comparisonExpression.getLeft();
                String leftExprName = left.getName().getSuffix();
                Literal right = (Literal)comparisonExpression.getRight();
                int columnIndex = schema.getFieldIndexByName(leftExprName);
                int columnVal = (Integer)row.getColumns().get(columnIndex);
                int rightValue = Integer.parseInt(comparisonExpression.getRight().toString());
                if(comparisonExpression.getType().equals(ComparisonExpression.Type.GREATER_THAN)) {
                    return columnVal > rightValue;
                } else if(comparisonExpression.getType().equals(ComparisonExpression.Type.LESS_THAN)) {
                    return columnVal < rightValue;
                } else if(comparisonExpression.getType().equals(ComparisonExpression.Type.EQUAL)) {
                    return columnVal == rightValue;
                }
                throw new RuntimeException("Invalid format.");
            }
        };
    }

}
