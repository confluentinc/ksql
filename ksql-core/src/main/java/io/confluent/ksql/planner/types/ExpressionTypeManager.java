package io.confluent.ksql.planner.types;

import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.PlanException;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;

public class ExpressionTypeManager extends DefaultASTVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

    final Schema schema;

    public ExpressionTypeManager(Schema schema) {
        this.schema = schema;
    }

    public Type getExpressionType(Expression expression) {
        ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
        process(expression, expressionTypeContext);
        return expressionTypeContext.getType();
    }

    class ExpressionTypeContext {
        Type type;

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }
    }

    @Override
    protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, ExpressionTypeContext expressionTypeContext)
    {
        process(node.getLeft(), expressionTypeContext);
        Type leftType = expressionTypeContext.getType();
        process(node.getRight(), expressionTypeContext);
        Type rightType = expressionTypeContext.getType();
        expressionTypeContext.setType(resolveArithmaticType(leftType, rightType));
        return null;
    }

    @Override
    protected Expression visitComparisonExpression(ComparisonExpression node, ExpressionTypeContext expressionTypeContext)
    {
        expressionTypeContext.setType(BooleanType.BOOLEAN);
        return null;
    }

    @Override
    protected Expression visitQualifiedNameReference(QualifiedNameReference node, ExpressionTypeContext expressionTypeContext)
    {
        SchemaField schemaField = schema.getFieldByName(node.getName().getSuffix());
        expressionTypeContext.setType(schemaField.getFieldType());
        return null;
    }

    private Type resolveArithmaticType(Type leftType, Type rightType) {
        if(leftType == rightType) {
            return  leftType;
        } else if((leftType == StringType.STRING) || (rightType == StringType.STRING)) {
            throw new PlanException("Incompatible types.");
        } else if((leftType == BooleanType.BOOLEAN) || (rightType == BooleanType.BOOLEAN)) {
            throw new PlanException("Incompatible types.");
        } else if((leftType == DoubleType.DOUBLE) || (rightType == DoubleType.DOUBLE)) {
            return DoubleType.DOUBLE;
        } else if((leftType == LongType.LONG) || (rightType == LongType.LONG)) {
            return LongType.LONG;
        } else if((leftType == IntegerType.INTEGER) || (rightType == IntegerType.INTEGER)) {
            return IntegerType.INTEGER;
        }

        throw new PlanException("Unsupported types.");
    }
}
