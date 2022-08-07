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
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class KsqlLogicalPlanner {
  private final Planner planner;

  public KsqlLogicalPlanner(final KsqlCatalog ksqlCatalog) {
    final SqlParser.Config parserConfig =
        SqlParser
            .config()
            .withCaseSensitive(false)
            .withParserFactory(SqlDdlParserImpl::new);

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(ksqlCatalog.getSchema())
        .parserConfig(parserConfig)
        .build();

    this.planner = Frameworks.getPlanner(config);
  }

  public static class KsqlLogicalPlan {
    private final RelRoot relRoot;
    private final String originalStatement;

    public KsqlLogicalPlan(final RelRoot relRoot, final String originalStatement) {
      this.relRoot = relRoot;
      this.originalStatement = originalStatement;
    }

    public RelRoot getRelRoot() {
      return relRoot;
    }

    public RelNode getRelNode() {
      return relRoot.rel;
    }

    public String getOriginalStatement() {
      return originalStatement;
    }
  }

  public KsqlLogicalPlan getLogicalPlan(final String statement) {
    try {
      final SqlNode parsed = planner.parse(statement);
      final SqlNode validated = planner.validate(parsed);
      final RelRoot logicalPlan = planner.rel(validated);
      return new KsqlLogicalPlan(logicalPlan, statement);
    } catch (SqlParseException e) {
      throw new KsqlStatementException("Could not parse statement", statement, e);
    } catch (ValidationException e) {
      throw new KsqlStatementException("Could not validate statement", statement, e);
    } catch (RelConversionException e) {
      throw new KsqlStatementException("Could not plan statement", statement, e);
    } finally {
      planner.close();
      planner.reset();
    }
  }
}

