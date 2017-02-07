/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.analyzer;

import io.confluent.kql.parser.tree.Node;

public class AnalysisContext {

  public enum ParentType {
    SELECT("select"),
    SELECTITEM("selectitem"),
    INTO("into"),
    FROM("from"),
    WHERE("where"),
    GROUPBY("GROUPBY");
    private final String value;

    ParentType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  final Node parentNode;
  final ParentType parentType;

  public AnalysisContext(final Node parentNode, final ParentType parentType) {
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
