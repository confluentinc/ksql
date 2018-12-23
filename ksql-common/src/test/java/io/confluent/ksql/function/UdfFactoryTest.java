/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UdfFactoryTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private static final String functionName = "TestFunc";
  private final UdfFactory factory = new UdfFactory(TestFunc.class,
      new UdfMetadata(functionName, "", "", "", "internal", false));
  
  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    expectedException.expect(KafkaException.class);
    expectedException.expectMessage("Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), BIGINT]");

    factory.getFunction(ImmutableList.of(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA));
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
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("VARCHAR(STRING), null");
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    factory.getFunction(Arrays.asList(Schema.STRING_SCHEMA, null));
  }

  @Test
  public void shouldThrowExceptionWhenAtLeastOneArgumentOtherThanNullDoesntMatch() {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("BIGINT, null");
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    factory.getFunction(Arrays.asList(Schema.OPTIONAL_INT64_SCHEMA, null));
  }

  @Test
  public void shouldThrowWhenNullAndPrimitiveTypeArg() {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("VARCHAR(STRING), null");
    final KsqlFunction function = new KsqlFunction(Schema.STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA),
        functionName,
        TestFunc.class
    );
    factory.addFunction(function);
    factory.getFunction(Arrays.asList(Schema.STRING_SCHEMA, null));
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
    expectedException.expect(KafkaException.class);
    expectedException.expectMessage("as a function with the same name has been loaded from a different jar");
    factory.addFunction(new KsqlFunction(
        Schema.STRING_SCHEMA,
        Collections.<Schema>emptyList(),
        "TestFunc",
        TestFunc.class,
        ksqlConfig -> null,
        "",
        "not the same path"
    ));
  }

  private abstract class TestFunc implements Kudf {

  }
}