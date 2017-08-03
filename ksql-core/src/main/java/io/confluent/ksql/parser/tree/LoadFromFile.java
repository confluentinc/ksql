/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

public class LoadFromFile extends Statement {

  final String schemaFilePath;

  public LoadFromFile(Optional<NodeLocation> location, String catalogFilePath) {
    super(location);
    if (catalogFilePath.startsWith("'") && catalogFilePath.endsWith("'")) {
      this.schemaFilePath = catalogFilePath.substring(1, catalogFilePath.length() - 1);
    } else {
      this.schemaFilePath = catalogFilePath;
    }

  }

  public String getSchemaFilePath() {
    return schemaFilePath;
  }

  @Override
  public int hashCode() {
    return Objects.hash("ListStreams");
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .toString();
  }
}
