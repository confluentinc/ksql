package io.confluent.ksql.analyzer;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.DefaultTraversalVisitor;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.plan.SourceNode;

import java.util.List;
import java.util.Optional;

public class Analyzer extends DefaultTraversalVisitor<Schema, AnalysisContext> {

    Analysis analysis;
    MetaStore metaStore;

    public Analyzer(Analysis analysis, MetaStore metaStore) {
        this.analysis = analysis;
        this.metaStore = metaStore;
    }

    @Override
    protected Schema visitQuerySpecification(QuerySpecification node, AnalysisContext context) {

        process(node.getSelect() , new AnalysisContext(null, AnalysisContext.ParentType.SELECT));

        process(node.getInto().get() , new AnalysisContext(null, AnalysisContext.ParentType.INTO));
        process(node.getFrom().get() , new AnalysisContext(null, AnalysisContext.ParentType.FROM));
//        process(node.getWhere().get(), new AnalysisContext(null, AnalysisContext.ParentType.WHERE));

        analyzeWhere(node.getWhere().get(), context);
        return null;
    }

    @Override
    protected Schema visitTable(Table node, AnalysisContext context) {

        System.out.println(node.getName().getSuffix());
        if(context.getParentType() == AnalysisContext.ParentType.INTO) {
            KafkaTopic kafkaTopic = new KafkaTopic(node.getName().getSuffix(), null, DataSource.DataSourceType.STREAM, node.getName().getSuffix());
            analysis.setInto(kafkaTopic);
        } else if(context.getParentType().equals(AnalysisContext.ParentType.FROM)) {
            analysis.getFromDataSources().add(metaStore.getSource(node.getName().getSuffix()));
        }

//        DataSource dataSource = metaStore.getSource(node.getName().getSuffix());
//        KafkaTopic kafkaTopic = new KafkaTopic();
        return null;
    }


    @Override
    protected Schema visitSelect(Select node, AnalysisContext context)
    {
        ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            if (selectItem instanceof AllColumns) {
                // expand * and T.*
//                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();
//
//                List<Field> fields = tupleDescriptor.resolveFieldsWithPrefix(starPrefix);
//                if (fields.isEmpty()) {
//                    if (starPrefix.isPresent()) {
//                        throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
//                    }
//                    throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
//                }
//
//                for (Field field : fields) {
//                    int fieldIndex = tupleDescriptor.indexOf(field);
//                    FieldReference expression = new FieldReference(fieldIndex);
//                    outputExpressionBuilder.add(expression);
//                    ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, tupleDescriptor, context);
//
//                    Type type = expressionAnalysis.getType(expression);
//                    if (node.getSelect().isDistinct() && !type.isComparable()) {
//                        throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type);
//                    }
//                }
            }
            else if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                if(column.getExpression() instanceof QualifiedNameReference) {
                    analysis.addSelectItem((QualifiedNameReference)column.getExpression());

                } else {
                    throw new RuntimeException("Only single column is supported. ");
                }

//                process(column, context);
//                ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), tupleDescriptor, context);
//                analysis.recordSubqueries(node, expressionAnalysis);
//                outputExpressionBuilder.add(column.getExpression());
//
//                Type type = expressionAnalysis.getType(column.getExpression());
//                if (node.getSelect().isDistinct() && !type.isComparable()) {
//                    throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression());
//                }
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + selectItem.getClass().getName());
            }
        }

//        ImmutableList<Expression> result = outputExpressionBuilder.build();
//        analysis.setOutputExpressions(node, result);
//
//        for(SelectItem selectItem: node.getSelectItems()) {
//            process(selectItem, new AnalysisContext(null, AnalysisContext.ParentType.SELECT));
//        }
        return null;
    }

    @Override
    protected Schema visitQualifiedNameReference(QualifiedNameReference node, AnalysisContext context)
    {
        return visitExpression(node, context);
    }

    private DataSource analyzeFrom(QuerySpecification node, AnalysisContext context) {

        return null;
    }

//    visitTable(Table node, C context)

    private void analyzeWhere(Node node, AnalysisContext context) {
        if(node instanceof ComparisonExpression) {
            analysis.setWhereExpression((ComparisonExpression)node);
        }

        System.out.println(node.toString());
    }

    private void analyzeSelect(Select select, AnalysisContext context) {

    }


}
