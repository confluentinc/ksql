/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import io.confluent.ksql.util.KsqlStatementException;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class KsqlLang {
  private final Planner planner;

  public KsqlLang() {
    final SchemaPlus schema = Frameworks.createRootSchema(true);

    // NEEDED: add integration with metastore here.
    // NEEDED: register UDFs here.


    final SqlParser.Config parserConfig =
        SqlParser
            .config()
            .withCaseSensitive(false)
            .withParserFactory(SqlDdlParserImpl::new);

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .build();

    planner = Frameworks.getPlanner(config);
  }

  public RelRoot getLogicalPlan(final String statement) {
    planner.close();
    planner.reset();
    final SqlNode parsed;
    try {
      parsed = planner.parse(statement);
    } catch (SqlParseException e) {
      throw new KsqlStatementException(
          "Could not parse expression with the new front-end.",
          statement,
          e
      );
    }
    if (parsed instanceof SqlCreateTable) {
      throw new KsqlStatementException(
          "CREATE TABLE is not yet supported by the new front-end.",
          statement
      );
    } else if (parsed instanceof SqlCreateMaterializedView) {
      throw new KsqlStatementException(
          "CREATE MATERIALIZED VIEW is not yet supported by the new front-end.",
          statement
      );
    } else {
      return getLogicalPlan(statement, parsed);
    }
  }

  private RelRoot getLogicalPlan(final String statement, final SqlNode parsed) {
    final SqlNode validated;
    try {
      validated = planner.validate(parsed);
    } catch (ValidationException e) {
      throw new KsqlStatementException(
          "Could not validate expression with the new front-end.",
          statement,
          e
      );
    }
    final RelRoot logicalPlan;
    try {
      logicalPlan = planner.rel(validated);
    } catch (RelConversionException e) {
      throw new KsqlStatementException(
          "Could not plan expression with the new front-end.",
          statement,
          e
      );
    }
    return logicalPlan;
  }
}
