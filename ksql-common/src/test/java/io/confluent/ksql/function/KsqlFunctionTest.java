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

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlFunctionTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Function<KsqlConfig, Kudf> udfFactory;

  @Test
  public void shouldResolveGenericReturnType() {
    // Given:
    final KsqlFunction function = createFunction(
        GenericsUtil.generic("T").build(),
        ImmutableList.of(GenericsUtil.generic("T").build())
    );

    // When:
    final Schema returnType = function.getReturnType(ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA));

    // Then:
    assertThat(returnType, is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldResolveGenericReturnTypeFromArray() {
    // Given:
    final KsqlFunction function = createFunction(
        GenericsUtil.generic("T").build(),
        ImmutableList.of(GenericsUtil.array("T").build())
    );

    // When:
    final Schema returnType = function.getReturnType(
        ImmutableList.of(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build()));

    // Then:
    assertThat(returnType, is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldResolveGenericReturnTypeFromSecondArgument() {
    // Given:
    final KsqlFunction function = createFunction(
        GenericsUtil.generic("T").build(),
        ImmutableList.of(
            GenericsUtil.generic("S").build(),
            GenericsUtil.generic("T").build()
        )
    );

    // When:
    final Schema returnType = function.getReturnType(
        ImmutableList.of(
            Schema.OPTIONAL_STRING_SCHEMA,
            Schema.OPTIONAL_INT64_SCHEMA));

    // Then:
    assertThat(returnType, is(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldResolveGenericArrayReturnType() {
    // Given:
    final KsqlFunction function = createFunction(
        GenericsUtil.array("T").build(),
        ImmutableList.of(GenericsUtil.generic("T").build())
    );

    // When:
    final Schema returnType = function.getReturnType(ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA));

    // Then:
    assertThat(returnType, is(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()));
  }

  @Test
  public void shouldResolveGenericFromVariadicArgument() {
    // Given:
    final KsqlFunction function = createFunction(
        GenericsUtil.generic("T").build(),
        ImmutableList.of(GenericsUtil.array("T").build()),
        true
    );

    // When:
    final Schema returnType = function.getReturnType(ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA));

    // Then:
    assertThat(returnType, is(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test
  public void shouldThrowOnNonOptionalReturnType() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("KSQL only supports optional field types");

    // When:
    createFunction(Schema.INT32_SCHEMA, ImmutableList.of());
  }

  private KsqlFunction createFunction(final Schema returnSchema, final List<Schema> args) {
    return createFunction(returnSchema, args, false);
  }

  private KsqlFunction createFunction(
      final Schema returnSchema,
      final List<Schema> args,
      final boolean isVariadic
  ) {
    return KsqlFunction.create(
        returnSchema,
        args,
        "funcName",
        MyUdf.class,
        udfFactory,
        "the description",
        "path/udf/loaded/from.jar",
        isVariadic
    );
  }

  private static final class MyUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }
}