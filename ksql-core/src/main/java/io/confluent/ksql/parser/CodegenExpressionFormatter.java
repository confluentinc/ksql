package io.confluent.ksql.parser;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.SchemaField;
import io.confluent.ksql.planner.types.*;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class CodegenExpressionFormatter {

    private CodegenExpressionFormatter() {}

    static Schema schema;

    public static String formatExpression(Expression expression, Schema schema)
    {
        CodegenExpressionFormatter.schema = schema;
        return formatExpression(expression, true);
    }

    public static String formatExpression(Expression expression, boolean unmangleNames)
    {
        Pair<String, Type> expressionFormatterResult = new CodegenExpressionFormatter.Formatter().process(expression, unmangleNames);
        return expressionFormatterResult.getLeft();
    }


    public static class Formatter
            extends AstVisitor<Pair<String, Type>, Boolean>
    {
        @Override
        protected Pair<String, Type> visitNode(Node node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Pair<String, Type> visitExpression(Expression node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected Pair<String, Type> visitBooleanLiteral(BooleanLiteral node, Boolean unmangleNames)
        {
            return new Pair<>(String.valueOf(node.getValue()), BooleanType.BOOLEAN);
        }

        @Override
        protected Pair<String, Type> visitStringLiteral(StringLiteral node, Boolean unmangleNames)
        {
            return new Pair<>("\""+node.getValue()+"\"", StringType.STRING);
        }

        @Override
        protected Pair<String, Type> visitBinaryLiteral(BinaryLiteral node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return new Pair<>("X'" + node.toHexString() + "'", StringType.STRING);
        }

        @Override
        protected Pair<String, Type> visitLongLiteral(LongLiteral node, Boolean unmangleNames)
        {
            return new Pair<>(Long.toString(node.getValue()), LongType.LONG);
        }

        @Override
        protected Pair<String, Type> visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames)
        {
            return new Pair<>(Double.toString(node.getValue()), DoubleType.DOUBLE);
        }

        @Override
        protected Pair<String, Type> visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected Pair<String, Type> visitGenericLiteral(GenericLiteral node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return node.getType() + " " + node.getValue();
        }

        @Override
        protected Pair<String, Type> visitNullLiteral(NullLiteral node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return new Pair<>("null", StringType.STRING);
        }


        @Override
        protected Pair<String, Type> visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames)
        {
            String fieldName = formatQualifiedName(node.getName());
            SchemaField schemaField = schema.getFieldByName(fieldName);
            if(schemaField == null) {
                throw new KSQLException("Field not found: "+schemaField.getFieldName());
            }
            return new Pair<>(fieldName, schemaField.getFieldType());
        }

        @Override
        protected Pair<String, Type> visitSymbolReference(SymbolReference node, Boolean context)
        {
            String fieldName = formatIdentifier(node.getName());
            SchemaField schemaField = schema.getFieldByName(fieldName);
            if(schemaField == null) {
                throw new KSQLException("Field not found: "+schemaField.getFieldName());
            }
            return new Pair<>(fieldName, schemaField.getFieldType());
        }

        @Override
        protected Pair<String, Type> visitDereferenceExpression(DereferenceExpression node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            String baseString = process(node.getBase(), unmangleNames);
//            return baseString + "." + formatIdentifier(node.getFieldName());
        }

        private static String formatQualifiedName(QualifiedName name)
        {
            List<String> parts = new ArrayList<>();
            for (String part : name.getParts()) {
                parts.add(formatIdentifier(part));
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        public Pair<String, Type> visitFieldReference(FieldReference node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
            // add colon so this won't parse
//            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected Pair<String, Type> visitFunctionCall(FunctionCall node, Boolean unmangleNames)
        {
//            StringBuilder builder = new StringBuilder();
//
//            String arguments = joinExpressions(node.getArguments(), unmangleNames);
//            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
//                arguments = "*";
//            }
//            if (node.isDistinct()) {
//                arguments = "DISTINCT " + arguments;
//            }
//
//            builder.append(formatQualifiedName(node.getName()))
//                    .append('(').append(arguments).append(')');
//
//            if (node.getWindow().isPresent()) {
//                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), unmangleNames));
//            }
//
//            return builder.toString();
            throw new UnsupportedOperationException();
        }

        @Override
        protected Pair<String, Type> visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean unmangleNames)
        {
            if(node.getType() == LogicalBinaryExpression.Type.OR) {
                return new Pair<>(formatBinaryExpression(" || ", node.getLeft(), node.getRight(), unmangleNames), BooleanType.BOOLEAN);
            } else if(node.getType() == LogicalBinaryExpression.Type.AND) {
                return new Pair<>(formatBinaryExpression(" && ", node.getLeft(), node.getRight(), unmangleNames), BooleanType.BOOLEAN);
            }
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected Pair<String, Type> visitNotExpression(NotExpression node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return "(! " + process(node.getValue(), unmangleNames) + ")";
        }

        @Override
        protected Pair<String, Type> visitComparisonExpression(ComparisonExpression node, Boolean unmangleNames)
        {
            Pair<String, Type> left = process(node.getLeft(), unmangleNames);
            Pair<String, Type> right = process(node.getRight(), unmangleNames);
            if ((left.getRight() == StringType.STRING) || (right.getRight() == StringType.STRING)) {
                if(node.getType().getValue().equals("=")) {
                    return new Pair<>(left.getLeft()+".equalsIgnoreCase("+right.getLeft()+")", BooleanType.BOOLEAN);
                }
            }
            String typeStr = node.getType().getValue();
            if (typeStr.equalsIgnoreCase("=")) {
                typeStr = "==";
            }
            return new Pair<>("("+left.getLeft()+" "+typeStr+" "+right.getLeft()+")", BooleanType.BOOLEAN);
        }

        @Override
        protected Pair<String, Type> visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return "(" + process(node.getValue(), unmangleNames) + " IS NULL)";
        }

        @Override
        protected Pair<String, Type> visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return "(" + process(node.getValue(), unmangleNames) + " == null)";
        }

        @Override
        protected Pair<String, Type> visitArithmeticUnary(ArithmeticUnaryExpression node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            String value = process(node.getValue(), unmangleNames);
//
//            switch (node.getSign()) {
//                case MINUS:
//                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
//                    String separator = value.startsWith("-") ? " " : "";
//                    return "-" + separator + value;
//                case PLUS:
//                    return "+" + value;
//                default:
//                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
//            }
        }

        @Override
        protected Pair<String, Type> visitArithmeticBinary(ArithmeticBinaryExpression node, Boolean unmangleNames)
        {
            Pair<String, Type> left = process(node.getLeft(), unmangleNames);
            Pair<String, Type> right = process(node.getRight(), unmangleNames);
            return new Pair<>("(" + left.getLeft() + " " + node.getType().getValue() + " " + right.getLeft() + ")", DoubleType.DOUBLE);
//            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(), unmangleNames);
        }

        @Override
        protected Pair<String, Type> visitLikePredicate(LikePredicate node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            StringBuilder builder = new StringBuilder();
//
//            builder.append('(')
//                    .append(process(node.getValue(), unmangleNames))
//                    .append(" LIKE ")
//                    .append(process(node.getPattern(), unmangleNames));
//
//            if (node.getEscape() != null) {
//                builder.append(" ESCAPE ")
//                        .append(process(node.getEscape(), unmangleNames));
//            }
//
//            builder.append(')');
//
//            return builder.toString();
        }

        @Override
        protected Pair<String, Type> visitAllColumns(AllColumns node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            if (node.getPrefix().isPresent()) {
//                return node.getPrefix().get() + ".*";
//            }
//
//            return "*";
        }

        @Override
        protected Pair<String, Type> visitBetweenPredicate(BetweenPredicate node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
//            return "(" + process(node.getValue(), unmangleNames) + " BETWEEN " +
//                    process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(), unmangleNames) + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right, boolean unmangleNames)
        {
            return "(" + process(left, unmangleNames) + " " + operator + " " + process(right, unmangleNames) + ")";
        }

        private static String formatIdentifier(String s)
        {
            // TODO: handle escaping properly
//            return '"' + s + '"';
            return s ;
        }

        private String joinExpressions(List<Expression> expressions, boolean unmangleNames)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, unmangleNames))
                    .iterator());
        }
    }


}
