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

package io.confluent.ksql.function;

import static com.google.common.collect.ImmutableList.of;
import static io.confluent.ksql.function.KsqlScalarFunction.create;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import org.apache.kafka.common.KafkaException;
import org.junit.Test;

public class UdfFactoryTest {

  private static final String functionName = "TestFunc";
  private final UdfFactory factory = new UdfFactory(TestFunc.class,
      new UdfMetadata(functionName, "", "", "", FunctionCategory.OTHER, "internal"));

  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    // When:
    final Exception e = assertThrows(
        KafkaException.class,
        () -> factory.getFunction(of(STRING, BIGINT))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Function 'TestFunc' does not accept parameters (STRING, BIGINT)"));
  }

  @Test
  public void shouldThrowExceptionIfAddingFunctionWithDifferentPath() {
    // When:
    final Exception e = assertThrows(
        KafkaException.class,
        () -> factory.addFunction(create(
            (params, args) -> STRING,
            ParamTypes.STRING,
            emptyList(),
            FunctionName.of("TestFunc"),
            TestFunc.class,
            ksqlConfig -> null,
            "",
            "not the same path",
            false
        ))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "as a function with the same name has been loaded from a different jar"));
  }

  private abstract class TestFunc implements Kudf {

  }
}