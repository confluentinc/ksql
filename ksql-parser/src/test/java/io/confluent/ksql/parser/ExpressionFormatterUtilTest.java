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

package io.confluent.ksql.parser;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class ExpressionFormatterUtilTest {
  @Test
  public void shouldFormatStructWithColumnWithReservedWordName() {
    final SqlStruct struct = SqlStruct.builder()
        .field("END", SqlTypes.INTEGER)
        .build();

    assertThat(
        ExpressionFormatterUtil.formatExpression(new Type(struct)),
        equalTo("STRUCT<`END` INTEGER>"));
  }
}