/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.KsqlTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("tables")
@JsonSubTypes({})
public class TablesList extends KsqlEntity {
  private final Collection<SourceInfo.Table> tables;

  @JsonCreator
  public TablesList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("tables") Collection<SourceInfo.Table> tables
  ) {
    super(statementText);
    this.tables = tables;
  }

  public static TablesList fromKsqlTables(String statementText, Collection<KsqlTable> ksqlTables) {
    Collection<SourceInfo.Table> tableInfos =
        ksqlTables.stream().map(SourceInfo.Table::new).collect(Collectors.toList());
    return new TablesList(statementText, tableInfos);
  }

  public List<SourceInfo.Table> getTables() {
    return new ArrayList<>(tables);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TablesList)) {
      return false;
    }
    TablesList that = (TablesList) o;
    return Objects.equals(getTables(), that.getTables());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTables());
  }
}
