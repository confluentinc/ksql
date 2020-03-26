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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import java.util.Collections;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class UdfFactoryTest {

  private static final String functionName = "TestFunc";
  private final UdfFactory factory = new UdfFactory(TestFunc.class,
      new UdfMetadata(functionName, "", "", "", "internal", false));
  
  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    // When:
    final KafkaException e = assertThrows(
        KafkaException.class,
        () -> factory.getFunction(ImmutableList.of(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), BIGINT]"));
  }

  @Test
  public void shouldThrowExceptionIfAddingFunctionWithDifferentPath() {
    final KafkaException e = assertThrows(
        KafkaException.class,
        () -> factory.addFunction(KsqlFunction.create(
            Schema.OPTIONAL_STRING_SCHEMA,
            Collections.<Schema>emptyList(),
            "TestFunc",
            TestFunc.class,
            ksqlConfig -> null,
            "",
            "not the same path",
            false
        )));

    assertThat(e.getMessage(),
        containsString("as a function with the same name has been loaded from a different jar"));
  }

  private abstract class TestFunc implements Kudf {

  }
}