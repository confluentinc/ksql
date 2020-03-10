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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import org.apache.kafka.common.KafkaException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UdfFactoryTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private static final String functionName = "TestFunc";
  private final UdfFactory factory = new UdfFactory(TestFunc.class,
      new UdfMetadata(functionName, "", "", "", "internal"));
  
  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    expectedException.expect(KafkaException.class);
    expectedException.expectMessage("Function 'TestFunc' does not accept parameters (STRING, BIGINT)");

    factory.getFunction(ImmutableList.of(SqlTypes.STRING, SqlTypes.BIGINT));
  }

  @Test
  public void shouldThrowExceptionIfAddingFunctionWithDifferentPath() {
    expectedException.expect(KafkaException.class);
    expectedException.expectMessage("as a function with the same name has been loaded from a different jar");
    factory.addFunction(KsqlScalarFunction.create(
        (params, args) -> SqlTypes.STRING,
        ParamTypes.STRING,
        Collections.emptyList(),
        FunctionName.of("TestFunc"),
        TestFunc.class,
        ksqlConfig -> null,
        "",
        "not the same path",
        false
    ));
  }

  private abstract class TestFunc implements Kudf {

  }
}