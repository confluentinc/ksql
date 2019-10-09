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

package io.confluent.ksql.function.udtf.array;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

public class ExplodeIntegerArrayFunctionFactory extends TableFunctionFactory {

  private static final FunctionName NAME = FunctionName.of("EXPLODE");

  private static final List<List<Schema>> SUPPORTED_TYPES = ImmutableList
      .<List<Schema>>builder()
      .add(ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA))
      .build();

  public ExplodeIntegerArrayFunctionFactory() {
    super(NAME.name());
  }

  @SuppressWarnings("unchecked")
  @Override
  public KsqlTableFunction getProperTableFunction(final List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("EXPLODE function should have two arguments.");
    }

    final Schema param = argTypeList.get(0);
    if (param.type() != Type.ARRAY) {
      throw new IllegalArgumentException("Only arrays supported for now");
    }
    final Schema valueSchema = param.valueSchema();
    if (valueSchema.type() != Type.INT64) {
      throw new IllegalArgumentException("Only arrays of bigint supported for now");
    }


    return new ExplodeIntegerArrayUdtf(NAME, valueSchema, argTypeList, "Explodes an array");

  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return SUPPORTED_TYPES;
  }
}