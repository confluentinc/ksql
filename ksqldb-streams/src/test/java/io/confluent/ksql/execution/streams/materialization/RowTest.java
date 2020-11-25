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

package io.confluent.ksql.execution.streams.materialization;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.TableRowValidation.Validator;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RowTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.DOUBLE)
      .build();

  private static final Schema KEY_STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("k0", Schema.OPTIONAL_STRING_SCHEMA)
      .field("k1", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey("key", 11);

  private static final GenericRow A_VALUE = GenericRow.genericRow("v0-v", 1.0d);
  private static final long A_ROWTIME = 1283535L;

  @Mock
  private Validator validator;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPE() {
    new NullPointerTester()
        .setDefault(LogicalSchema.class, SCHEMA)
        .setDefault(GenericKey.class, A_KEY)
        .setDefault(GenericRow.class, A_VALUE)
        .testStaticMethods(Row.class, Visibility.PROTECTED);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEquals() {
    final LogicalSchema differentSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("diff0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("diff1"), SqlTypes.DOUBLE)
        .build();

    new EqualsTester()
        .addEqualityGroup(
            Row.of(SCHEMA, A_KEY, A_VALUE, A_ROWTIME),
            Row.of(SCHEMA, A_KEY, A_VALUE, A_ROWTIME)
        )
        .addEqualityGroup(
            Row.of(differentSchema, A_KEY, A_VALUE, A_ROWTIME)
        )
        .addEqualityGroup(
            Row.of(SCHEMA, GenericKey.genericKey("diff", 11), A_VALUE, A_ROWTIME)
        )
        .addEqualityGroup(
            Row.of(SCHEMA, A_KEY, GenericRow.genericRow(null, null), A_ROWTIME)
        )
        .addEqualityGroup(
            Row.of(SCHEMA, A_KEY, A_VALUE, -1L)
        )
        .testEquals();
  }

  @Test
  public void shouldValidateOnConstruction() {
    // When:
    new Row(SCHEMA, A_KEY, A_VALUE, A_ROWTIME, validator);

    // Then:
    verify(validator).validate(SCHEMA, A_KEY, A_VALUE);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  @Test
  public void shouldValidateOnCopy() {
    // Given:
    final Row row = new Row(SCHEMA, A_KEY, A_VALUE, A_ROWTIME, validator);
    clearInvocations(validator);

    // When:
    row.withValue(A_VALUE, SCHEMA);

    // Then:
    verify(validator).validate(SCHEMA, A_KEY, A_VALUE);
  }
}