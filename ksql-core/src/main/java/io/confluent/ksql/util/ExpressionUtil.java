package io.confluent.ksql.util;


import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.util.HashMap;
import java.util.Map;

public class ExpressionUtil {

    public Map<String, Class> getParameterInfo(Expression expression, KSQLSchema schema) {
        Visitor visitor = new Visitor(schema);
        visitor.process(expression, null);
        return visitor.parameterMap;
    }

    public Pair<IExpressionEvaluator, int[]> getExpressionEvaluator(Expression expression, KSQLSchema schema) throws Exception {
        ExpressionUtil expressionUtil = new ExpressionUtil();
        Map<String, Class> parameterMap = expressionUtil.getParameterInfo(expression, schema);

        String[] parameterNames = new String[parameterMap.size()];
        Class[] parameterTypes = new Class[parameterMap.size()];
        int[] columnIndexes = new int[parameterMap.size()];

        int index = 0;
        for (String parameterName: parameterMap.keySet()) {
            parameterNames[index] = parameterName;
            parameterTypes[index] = parameterMap.get(parameterName);
            columnIndexes[index] = schema.getFieldIndexByName(parameterName);
            index++;
        }

        String expressionStr = expression.getCodegenString(schema);
        IExpressionEvaluator ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

        // The expression will have two "int" parameters: "a" and "b".
        ee.setParameters(parameterNames, parameterTypes);

        // And the expression (i.e. "result") type is also "int".
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema.Type expressionType = expressionTypeManager.getExpressionType(expression);

        ee.setExpressionType(getJavaType(expressionType));

        // And now we "cook" (scan, parse, compile and load) the fabulous expression.
        ee.cook(expressionStr);

        return new Pair<>(ee, columnIndexes);
    }

    public Class getJavaType(Schema.Type type) {
        if(type == Schema.Type.BOOLEAN) {
            return Boolean.class;
        } else if(type == Schema.Type.INT32) {
            return Integer.class;
        } else if(type == Schema.Type.INT64) {
            return Long.class;
        } else if(type == Schema.Type.FLOAT64) {
            return Double.class;
        } else if(type == Schema.Type.STRING) {
            return String.class;
        }
        throw new KSQLException("Type is not supported: "+type);
    }


    private class Visitor
            extends AstVisitor<Object, Object> {

        final KSQLSchema schema;
        final Map<String, Class> parameterMap;

        Visitor(KSQLSchema schema) {
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
            Field schemaField = schema.getFieldByName(node.getName().getSuffix());
            if (schemaField == null) {
                throw new RuntimeException("Cannot find the select field in the available fields: " + node.getName().getSuffix());
            }
            parameterMap.put(schemaField.name(), getJavaType(schemaField.schema().type()));
            return null;
        }
    }

}
