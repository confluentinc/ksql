/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.inference;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;

/**
 * Type for injecting schemas into statements that have none.
 */
public interface SchemaInjector {

  /**
   * Attempt to inject the schema into the supplied {@code statement}.
   *
   * <p>The schema is only injected if:
   * <ul>
   * <li>The statement is a CT/CS.</li>
   * <li>The statement does not defined a schema.</li>
   * <li>The format of the statement supports schema inference.</li>
   * </ul>
   *
   * <p>If any of the above are not true then the {@code statement} is returned unchanged.
   *
   * @param statement the statement to potentially inject a schema into.
   * @param <T> the type of the statement.
   * @return a statement that potentially has had the schema injected.
   */
  <T extends Statement> PreparedStatement<T> forStatement(PreparedStatement<T> statement);
}
