package io.confluent.ksql.parser;


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.*;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class CodegenExpressionFormatter {

    private CodegenExpressionFormatter() {}

    public static String formatExpression(Expression expression)
    {
        return formatExpression(expression, true);
    }

    public static String formatExpression(Expression expression, boolean unmangleNames)
    {
        return new CodegenExpressionFormatter.Formatter().process(expression, unmangleNames);
    }
    public static class Formatter
            extends AstVisitor<String, Boolean>
    {
        @Override
        protected String visitNode(Node node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Boolean unmangleNames)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Boolean unmangleNames)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Boolean unmangleNames)
        {
            return "'"+node.getValue()+"'";
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Boolean unmangleNames)
        {
            return "X'" + node.toHexString() + "'";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Boolean unmangleNames)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames)
        {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames)
        {
            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Boolean unmangleNames)
        {
            return node.getType() + " " + node.getValue();
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Boolean unmangleNames)
        {
            return "null";
        }


        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Boolean unmangleNames)
        {
            return formatQualifiedName(node.getName());
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Boolean context)
        {
            return formatIdentifier(node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Boolean unmangleNames)
        {
            String baseString = process(node.getBase(), unmangleNames);
            return baseString + "." + formatIdentifier(node.getFieldName());
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
        public String visitFieldReference(FieldReference node, Boolean unmangleNames)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Boolean unmangleNames)
        {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments(), unmangleNames);
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatQualifiedName(node.getName()))
                    .append('(').append(arguments).append(')');

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), unmangleNames));
            }

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean unmangleNames)
        {
            if(node.getType() == LogicalBinaryExpression.Type.OR) {
                return formatBinaryExpression(" || ", node.getLeft(), node.getRight(), unmangleNames);
            } else if(node.getType() == LogicalBinaryExpression.Type.AND) {
                return formatBinaryExpression(" && ", node.getLeft(), node.getRight(), unmangleNames);
            }
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitNotExpression(NotExpression node, Boolean unmangleNames)
        {
            return "(! " + process(node.getValue(), unmangleNames) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Boolean unmangleNames)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(), unmangleNames);
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames)
        {
            return "(" + process(node.getValue(), unmangleNames) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames)
        {
            return "(" + process(node.getValue(), unmangleNames) + " == null)";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Boolean unmangleNames)
        {
            String value = process(node.getValue(), unmangleNames);

            switch (node.getSign()) {
                case MINUS:
                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                    String separator = value.startsWith("-") ? " " : "";
                    return "-" + separator + value;
                case PLUS:
                    return "+" + value;
                default:
                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
            }
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Boolean unmangleNames)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(), unmangleNames);
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Boolean unmangleNames)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), unmangleNames))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), unmangleNames));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                        .append(process(node.getEscape(), unmangleNames));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Boolean unmangleNames)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Boolean unmangleNames)
        {
            return "(" + process(node.getValue(), unmangleNames) + " BETWEEN " +
                    process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(), unmangleNames) + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right, boolean unmangleNames)
        {
            return '(' + process(left, unmangleNames) + ' ' + operator + ' ' + process(right, unmangleNames) + ')';
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
