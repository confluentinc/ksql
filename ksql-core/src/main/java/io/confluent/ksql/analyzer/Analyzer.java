package io.confluent.ksql.analyzer;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.DefaultTraversalVisitor;
import org.apache.kafka.connect.data.Schema;

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

        if(node.getWhere().isPresent()) {
            analyzeWhere(node.getWhere().get(), context);
        }

        return null;
    }

    @Override
    protected Schema visitTable(Table node, AnalysisContext context) {

        if(context.getParentType() == AnalysisContext.ParentType.INTO) {
            KafkaTopic kafkaTopic = new KafkaTopic(node.getName().getSuffix(), null, DataSource.DataSourceType.STREAM, node.getName().getSuffix());
            analysis.setInto(kafkaTopic);
        } else if(context.getParentType().equals(AnalysisContext.ParentType.FROM)) {
            analysis.getFromDataSources().add(metaStore.getSource(node.getName().getSuffix().toLowerCase()));
        }

        return null;
    }


    @Override
    protected Schema visitSelect(Select node, AnalysisContext context)
    {
        ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            if (selectItem instanceof AllColumns) {
                // expand * and T.*
                throw new IllegalArgumentException("Unsupported SelectItem type: " + selectItem.getClass().getName());

            }
            else if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                analysis.addSelectItem(column.getExpression(), column.getAlias().get());
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + selectItem.getClass().getName());
            }
        }

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

    private void analyzeWhere(Node node, AnalysisContext context) {
        analysis.setWhereExpression((Expression) node);
        System.out.println(node.toString());
    }

    private void analyzeSelect(Select select, AnalysisContext context) {

    }


}
