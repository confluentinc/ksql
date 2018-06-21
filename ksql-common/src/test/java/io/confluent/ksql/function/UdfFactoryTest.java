/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableList;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;

public class UdfFactoryTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private UdfFactory factory;

  @Before
  public void setUp() {
    factory = new UdfFactory(TestFunc.class, new UdfMetadata("TestFunc", "", "", "", ""));
  }

  @Test
  public void shouldThrowIfNoVariantFoundThatAcceptsSuppliedParamTypes() {
    expectedException.expect(KafkaException.class);
    expectedException.expectMessage("Function 'TestFunc' does not accept parameters of types:[VARCHAR(STRING), BIGINT]");

    factory.getFunction(ImmutableList.of(Schema.Type.STRING, Schema.Type.INT64));
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
        () -> null,
        "",
        "not the same path"
    ));
  }

  private abstract class TestFunc implements Kudf {

  }
}