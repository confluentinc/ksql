/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class StructTerm implements Term {

  private final Map<String, Term> nameToTermMap;
  private final SqlType resultType;

  public StructTerm(final Map<String, Term> nameToTermMap, final SqlType resultType) {
    this.nameToTermMap = nameToTermMap;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final Schema schema = SchemaConverters
        .sqlToConnectConverter()
        .toConnectSchema(resultType);
    final Struct struct = new Struct(schema);
    for (Map.Entry<String, Term> entry : nameToTermMap.entrySet()) {
      struct.put(entry.getKey(), entry.getValue().getValue(context));
    }
    return struct;
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
