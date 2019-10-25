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

package io.confluent.ksql.function.tf;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.udtf.array.ExplodeFunctionFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class ExplodeArrayTableFunctionTest {

  private ExplodeFunctionFactory factory;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    factory = new ExplodeFunctionFactory();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCreateTableFunction() {
    KsqlTableFunction<Integer, Integer> tf =
        (KsqlTableFunction<Integer, Integer>)factory.createTableFunction(intListParamTypes());
    assertThat(tf, is(notNullValue()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFlatMapArray() {
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6);
    KsqlTableFunction tf = createTableFunction();
    List<Integer> output = tf.flatMap(input);
    assertThat(input, is(output));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFlatMapEmptyArray() {
    List<Integer> input = Collections.emptyList();
    KsqlTableFunction tf = createTableFunction();
    List<Integer> output = tf.flatMap(input);
    assertThat(input, is(output));
  }

  private static List<Schema> intListParamTypes() {
    Schema schema = SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA).build();
    return Collections.singletonList(schema);
  }

  @SuppressWarnings("unchecked")
  private KsqlTableFunction<List<Integer>, Integer> createTableFunction() {
    return (KsqlTableFunction<List<Integer>, Integer>)factory.createTableFunction(intListParamTypes());
  }
}