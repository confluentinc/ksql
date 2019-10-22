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

import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

public class ExplodeFunctionFactory extends TableFunctionFactory {

  private static final FunctionName NAME = FunctionName.of("EXPLODE");

  public ExplodeFunctionFactory() {
    super(new UdfMetadata(NAME.name(),
        "",
        KsqlConstants.CONFLUENT_AUTHOR,
        "",
        "",
        false));
  }

  @SuppressWarnings("unchecked")
  @Override
  public KsqlTableFunction<?, ?> createTableFunction(final List<Schema> argTypeList) {
    if (argTypeList.size() != 1) {
      throw new KsqlException("EXPLODE function should have one arguments.");
    }

    final Schema schema = argTypeList.get(0);
    if (schema.type() == Type.ARRAY) {
      return new ExplodeTableFunction(NAME, schema.valueSchema(), argTypeList,
          "Explodes an array");
    }
    throw new KsqlException("Unsupported argument type for EXPLODE " + schema);
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return null;
  }
}