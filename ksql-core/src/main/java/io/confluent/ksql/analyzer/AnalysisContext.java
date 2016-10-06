package io.confluent.ksql.analyzer;

import io.confluent.ksql.parser.tree.Node;

public class AnalysisContext {

    public enum ParentType
    {
        SELECT("select"),
        SELECTITEM("selectitem"),
        INTO("into"),
        FROM("from"),
        WHERE("where"),
        GROUPBY("GROUPBY");
        private final String value;

        ParentType(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }
    final Node parentNode;
    final ParentType parentType;

    public AnalysisContext(Node parentNode, ParentType parentType) {
        this.parentNode = parentNode;
        this.parentType = parentType;
    }

    public Node getParentNode() {
        return parentNode;
    }

    public ParentType getParentType() {
        return parentType;
    }

}
