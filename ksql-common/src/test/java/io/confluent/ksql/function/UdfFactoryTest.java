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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class UdfFactoryTest {

  private final String functionName = "TestFunc";
  private final UdfFactory factory = new UdfFactory(TestFunc.class,
      new UdfMetadata(functionName, "", "", "", "internal", false));
  
  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    Exception e = assertThrows(KafkaException.class,
        () -> factory.getFunction(ImmutableList.of(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA)));
    assertEquals("Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), BIGINT]", e.getMessage());
  }

  @Test
  public void shouldFindFirstMatchingFunctionWhenNullTypeInArgs() {
    final KsqlFunction expected = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(expected);
    factory.addFunction(new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        functionName,
        TestFunc.class
    ));

    final KsqlFunction function = factory.getFunction(Collections.singletonList(null));
    assertThat(function, equalTo(expected));
  }

  @Test
  public void shouldNotMatchingFunctionWhenNullTypeInArgsIfParamLengthsDiffer() {
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    Exception e = assertThrows(KsqlException.class,
        () -> factory.getFunction(Arrays.asList(Schema.STRING_SCHEMA, null)));
    assertEquals("Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), null]", e.getMessage());
  }

  @Test
  public void shouldThrowExceptionWhenAtLeastOneArgumentOtherThanNullDoesntMatch() {
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    Exception e = assertThrows(KsqlException.class,
        () -> factory.getFunction(Arrays.asList(Schema.OPTIONAL_INT64_SCHEMA, null)));
    assertEquals("Function 'TestFunc' does not accept parameters of types:[BIGINT, null]", e.getMessage());
  }

  @Test
  public void shouldThrowWhenNullAndPrimitiveTypeArg() {
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    Exception e = assertThrows(KsqlException.class,
        () -> factory.getFunction(Arrays.asList(Schema.STRING_SCHEMA, null)));
    assertEquals("Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), null]", e.getMessage());
  }

  @Test
  public void shouldMatchNullWithStringSchema() {
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Arrays.asList(Schema.INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    factory.getFunction(Arrays.asList(Schema.OPTIONAL_INT64_SCHEMA, null));
  }

  @Test
  public void shouldThrowExceptionIfAddingFunctionWithDifferentPath() {
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.emptyList(), "TestFunc", TestFunc.class, ksqlConfig -> null, "",
        "not the same path"
    );
    Exception e = assertThrows(KafkaException.class,
        () -> factory.addFunction(function));
    assertThat(e.getMessage(), containsString("as a function with the same name has been loaded from a different jar"));
  }

  private abstract class TestFunc implements Kudf {

  }
}