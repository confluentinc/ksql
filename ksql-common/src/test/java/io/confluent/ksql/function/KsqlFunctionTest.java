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

import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
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
  public void shouldThrowOnNonOptionalReturnType() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("KSQL only supports optional field types");

    // When:
    KsqlFunction.create(
        Schema.INT32_SCHEMA, // <-- non-optional return type.
        Collections.emptyList(),
        "funcName",
        MyUdf.class,
        udfFactory,
        "the description",
        "path/udf/loaded/from.jar",
        false

    );
  }

  private static final class MyUdf implements Kudf {

    @Override
    public Object evaluate(final Object... args) {
      return null;
    }
  }
}