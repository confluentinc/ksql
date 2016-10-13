package io.confluent.ksql.util;


import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.*;
import io.confluent.ksql.planner.operators.BigintOperators;
import io.confluent.ksql.planner.operators.DoubleOperators;
import io.confluent.ksql.planner.operators.IntegerOperators;
import io.confluent.ksql.planner.types.*;

import java.util.HashMap;
import java.util.Map;

public class ExpressionUtil {

    public Map<String, Class> getParameterInfo(Expression expression, Schema schema) {
        Visitor visitor = new Visitor(schema);
        visitor.process(expression, null);
        return visitor.parameterMap;
    }

    private class Visitor
            extends AstVisitor<Object, Object> {

        final Schema schema;
        final Map<String, Class> parameterMap;

        Visitor(Schema schema) {
            this.schema = schema;
            this.parameterMap = new HashMap<>();
        }

        protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context)
        {
            process(node.getLeft(), null);
            process(node.getRight(), null);
            return null;
        }

        protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context)
        {
            process(node.getLeft(), null);
            process(node.getRight(), null);
            return null;
        }
        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            process(node.getLeft(), null);
            process(node.getRight(), null);
            return null;
        }

        @Override
        protected Object visitNotExpression(NotExpression node, Object context)
        {
            return process(node.getValue(), null);
        }

        @Override
        protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context) {
            SchemaField schemaField = schema.getFieldByName(node.getName().getSuffix());
            if (schemaField == null) {
                throw new RuntimeException("Cannot find the select field in the available fields: " + node.getName().getSuffix());
            }
            parameterMap.put(schemaField.getFieldName(), schemaField.getFieldType().getJavaType());
            return null;
        }
    }

}
