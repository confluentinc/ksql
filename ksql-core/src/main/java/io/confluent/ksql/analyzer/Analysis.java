package io.confluent.ksql.analyzer;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.parser.tree.Expression;

import java.util.ArrayList;
import java.util.List;

public class Analysis {


    DataSource into;
    List<DataSource> fromDataSources = new ArrayList<>();
    Expression whereExpression = null;
    List<Expression> selectExpressions = new ArrayList<>();
    List<String> selectExpressionAlias = new ArrayList<>();

    public void addSelectItem(Expression expression, String alias) {
        selectExpressions.add(expression);
        selectExpressionAlias.add(alias);
    }

    public DataSource getInto() {
        return into;
    }

    public void setInto(DataSource into) {
        this.into = into;
    }

    public List<DataSource> getFromDataSources() {
        return fromDataSources;
    }

    public void setFromDataSources(List<DataSource> fromDataSources) {
        this.fromDataSources = fromDataSources;
    }

    public Expression getWhereExpression() {
        return whereExpression;
    }

    public void setWhereExpression(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    public List<Expression> getSelectExpressions() {
        return selectExpressions;
    }

    public void setSelectExpressions(List<Expression> selectExpressions) {
        this.selectExpressions = selectExpressions;
    }

    public List<String> getSelectExpressionAlias() {
        return selectExpressionAlias;
    }

    public void setSelectExpressionAlias(List<String> selectExpressionAlias) {
        this.selectExpressionAlias = selectExpressionAlias;
    }
}
